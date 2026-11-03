#!/usr/bin/env python3
"""Validate deployment archive contents without extracting or executing them."""
import hashlib
import json
import re
import struct
import sys
import tarfile


# 解析二进制结构前校验偏移与长度，避免越界读取或负长度绕过边界。
def bounded(offset, size, length):
    if offset < 0 or size < 0 or offset > length or size > length - offset:
        raise ValueError("binary structure outside file")


# 发布归档核验 Linux 程序时检查 ELF64 架构、段、入口与依赖，拒绝动态解释器和共享库需求。
def validate_elf(data, machine):
    if len(data) < 64 or data[:7] != b"\x7fELF\x02\x01\x01":
        raise ValueError("incomplete ELF64 header")
    kind, actual_machine, version, entry, phoff, shoff, _, ehsize, phsize, phnum, shsize, shnum, shstr = struct.unpack_from("<HHIQQQIHHHHHH", data, 16)
    if kind not in (2, 3) or actual_machine != machine or version != 1 or ehsize != 64 or phsize != 56 or not 0 < phnum < 65535:
        raise ValueError("invalid ELF executable header")
    bounded(phoff, phsize * phnum, len(data))
    executable_entry = False
    for index in range(phnum):
        segment, flags, offset, address, _, filesz, memsz, align = struct.unpack_from("<IIQQQQQQ", data, phoff + index * phsize)
        bounded(offset, filesz, len(data))
        if segment == 3:  # PT_INTERP requires a runtime dynamic loader.
            raise ValueError("Linux package contains a dynamic interpreter")
        if segment == 1:
            if filesz > memsz or (align > 1 and (align & (align - 1) or offset % align != address % align)):
                raise ValueError("invalid ELF load segment")
            executable_entry |= bool(flags & 1 and address <= entry < address + filesz)
        if segment == 2:  # Static PIE may have PT_DYNAMIC, but no DT_NEEDED.
            if not filesz or filesz % 16:
                raise ValueError("invalid ELF dynamic table")
            terminated = False
            for pos in range(offset, offset + filesz, 16):
                tag, _ = struct.unpack_from("<qQ", data, pos)
                if tag == 1:
                    raise ValueError("Linux package requires a shared library")
                if tag == 0:
                    terminated = True
                    break
            if not terminated:
                raise ValueError("unterminated ELF dynamic table")
    if not executable_entry:
        raise ValueError("ELF entry is not in a file-backed executable segment")
    if shoff or shnum:
        if not shnum or shsize != 64 or shstr >= shnum:
            raise ValueError("invalid ELF section table")
        bounded(shoff, shsize * shnum, len(data))
        for index in range(shnum):
            _, section_type, _, _, offset, size = struct.unpack_from("<IIQQQQ", data, shoff + index * shsize)
            if section_type != 8:  # SHT_NOBITS has no file bytes.
                bounded(offset, size, len(data))


# 发布归档核验 macOS 程序时检查 ARM64 可执行结构、入口及加载命令，仅允许系统库路径。
def validate_macho(data):
    if len(data) < 32 or data[:4] != b"\xcf\xfa\xed\xfe":
        raise ValueError("incomplete Mach-O header")
    cpu, _, kind, count, command_bytes, _, _ = struct.unpack_from("<IIIIIII", data, 4)
    if cpu != 0x0100000c or kind != 2 or not count or count > command_bytes // 8:
        raise ValueError("invalid Mach-O executable header")
    bounded(32, command_bytes, len(data))
    position = 32
    executable_ranges = []
    entry = None
    for _ in range(count):
        bounded(position, 8, 32 + command_bytes)
        command, size = struct.unpack_from("<II", data, position)
        if size < 8 or size % 8:
            raise ValueError("invalid Mach-O load command size")
        bounded(position, size, 32 + command_bytes)
        if command == 0x19:  # LC_SEGMENT_64
            if size < 72:
                raise ValueError("incomplete Mach-O segment")
            _, vmsize, offset, filesz, _, protection, sections, _ = struct.unpack_from("<QQQQIIII", data, position + 24)
            if size != 72 + sections * 80 or filesz > vmsize:
                raise ValueError("invalid Mach-O segment structure")
            bounded(offset, filesz, len(data))
            if protection & 4 and filesz:
                executable_ranges.append((offset, offset + filesz))
            for index in range(sections):
                section = position + 72 + index * 80
                _, section_size, section_offset = struct.unpack_from("<QQI", data, section + 32)
                section_type = struct.unpack_from("<I", data, section + 64)[0] & 0xff
                if section_type not in (1, 0xc, 0x12):  # zero-fill section types
                    bounded(section_offset, section_size, len(data))
                    if section_offset < offset or section_offset + section_size > offset + filesz:
                        raise ValueError("Mach-O section outside segment")
        elif command == 0x80000028:  # LC_MAIN
            if size != 24 or entry is not None:
                raise ValueError("invalid Mach-O entry command")
            entry = struct.unpack_from("<Q", data, position + 8)[0]
        elif command in (0xc, 0x18, 0x80000018, 0x8000001f, 0x80000023):
            if size < 24:
                raise ValueError("invalid Mach-O dylib command")
            name_offset = struct.unpack_from("<I", data, position + 8)[0]
            if not 24 <= name_offset < size:
                raise ValueError("invalid Mach-O dylib name")
            name = data[position + name_offset:position + size].split(b"\0", 1)[0]
            if not name.startswith((b"/usr/lib/", b"/System/Library/")):
                raise ValueError("Mach-O requires a non-system library")
        elif command in (0x1d, 0x1e, 0x26, 0x29, 0x2b, 0x2e, 0x80000033, 0x80000034):
            if size != 16:
                raise ValueError("invalid Mach-O linkedit command")
            offset, length = struct.unpack_from("<II", data, position + 8)
            bounded(offset, length, len(data))
        position += size
    if position != 32 + command_bytes or entry is None or not any(start <= entry < end for start, end in executable_ranges):
        raise ValueError("invalid Mach-O executable entry/load structure")


# 上传草稿前读取归档，核对安全路径、完整清单、文件哈希和目标二进制，再返回来源记录供发布入口复核。
def validate(archive, root, platform):
    binary = f"bin/kafka-dashboard-{platform}"
    required = {binary, "start.sh", ".env.example", "README.txt", "RELEASE.json", "SHA256SUMS", "web/index.html"}
    files = {}
    names = set()
    # 仅从归档读取普通文件内容，不解压或执行；先验证路径和类型，防止链接、敏感文件和越界成员混入。
    with tarfile.open(archive, "r:gz") as package:
        for member in package:
            name = member.name.rstrip("/")
            if name in names:
                raise ValueError("duplicate archive entry")
            names.add(name)
            if name == root and member.isdir():
                continue
            if not name.startswith(root + "/"):
                raise ValueError("invalid archive root")
            path = name[len(root) + 1:]
            if any(part in ("", ".", "..") for part in path.split("/")) or re.search(r"[\\\x00-\x1f\x7f]", path):
                raise ValueError("unsafe archive path")
            if member.isdir():
                if path not in ("bin", "web", "web/assets"):
                    raise ValueError("unexpected archive directory")
                continue
            if not member.isfile() or member.mode & 0o7000:
                raise ValueError("special archive file")
            if path not in required and path != "web/vite.svg" and not re.fullmatch(r"web/assets/[^/]+\.(js|css|svg|png|jpg|woff2)", path):
                raise ValueError("unexpected archive file")
            if any(part.startswith(".") or part in ("data", "node_modules") for part in path.split("/")) and path != ".env.example":
                raise ValueError("sensitive archive path")
            stream = package.extractfile(member)
            if path in (binary, "start.sh") and not member.mode & 0o111:
                raise ValueError("package executable permission missing")
            files[path] = stream.read()
    if not required.issubset(files) or not any(name.startswith("web/assets/") for name in files):
        raise ValueError("incomplete deployment package")
    checksums = {}
    for line in files["SHA256SUMS"].decode("utf-8").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if not match:
            raise ValueError("invalid internal checksum manifest")
        name = match[2].removeprefix("./")
        if name in checksums:
            raise ValueError("duplicate internal checksum entry")
        checksums[name] = match[1]
    # 先要求清单精确覆盖全部文件，再逐项计算字节哈希，避免漏列文件绕过完整性校验。
    if set(checksums) != set(files) - {"SHA256SUMS"}:
        raise ValueError("incomplete internal checksum manifest")
    for name, digest in checksums.items():
        if hashlib.sha256(files[name]).hexdigest() != digest:
            raise ValueError("internal checksum mismatch")
    # 完整性确认后按目标平台解析二进制结构；此静态门禁仍不能替代各平台的真实启动验证。
    if platform.startswith("linux-"):
        validate_elf(files[binary], 62 if platform == "linux-amd64" else 183)
    else:
        validate_macho(files[binary])
    return json.loads(files["RELEASE.json"])


if __name__ == "__main__":
    try:
        # 所有检查成功才输出来源 JSON，让发布入口继续核对标签、版本与干净状态。
        print(json.dumps(validate(*sys.argv[1:])))
    except (ValueError, OSError, tarfile.TarError, KeyError, UnicodeError, struct.error) as error:
        sys.exit(f"archive validation failed: {error}")
