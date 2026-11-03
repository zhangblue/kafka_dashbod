import assert from 'node:assert/strict'
import { readdirSync, readFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import ts from 'typescript'

const sourceRoot = fileURLToPath(new URL('../src/', import.meta.url))
// 品牌、语言自称、Kafka 术语与装饰符号无需翻译；普通文案必须进入资源。
const retainedText = new Set(['Kafka Lens', 'K', '中文', '中', 'English', 'EN', 'Consumer Group', 'Topic',
  '⌁', '◫', '◉', '⚙', '×', '⚠', '/', '＋', '·', ':', '●', '↻', '⌄', '›', '↑', '↓', '→', '↗', '↘', '—'])
const visibleAttributes = new Set(['aria-label', 'aria-description', 'title', 'placeholder', 'alt', 'label', 'description'])

// 审计时递归枚举全部 TypeScript 源码；测试、fixture 与翻译资源分别归类，不误报为生产文案。
function sourceFiles(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const file = path.join(directory, entry.name)
    return entry.isDirectory() ? sourceFiles(file) : /\.tsx?$/.test(file) ? [file] : []
  })
}

// 使用 TypeScript AST 收集直接可见的静态文本，包含 JSX 表达式中的条件与模板字符串。
function visibleLiterals(source) {
  const values = []
  // 字符串位于可见表达式时记录字面值；翻译调用参数是 key 或用户数据，不是直接 UI 文案。
  function collectExpression(node) {
    if (ts.isStringLiteralLike(node) || ts.isTemplateHead(node) || ts.isTemplateMiddle(node) || ts.isTemplateTail(node)) {
      if (node.text.trim()) values.push(node.text.trim())
    } else if (ts.isJsxExpression(node) && node.expression) {
      collectExpression(node.expression)
    } else if (ts.isConditionalExpression(node)) {
      collectExpression(node.whenTrue)
      collectExpression(node.whenFalse)
    } else if (ts.isParenthesizedExpression(node)) {
      collectExpression(node.expression)
    } else if (ts.isTemplateExpression(node)) {
      collectExpression(node.head)
      for (const span of node.templateSpans) { collectExpression(span.expression); collectExpression(span.literal) }
    } else if (ts.isBinaryExpression(node)) {
      if ([ts.SyntaxKind.BarBarToken, ts.SyntaxKind.QuestionQuestionToken].includes(node.operatorToken.kind)) collectExpression(node.left)
      if ([ts.SyntaxKind.AmpersandAmpersandToken, ts.SyntaxKind.BarBarToken, ts.SyntaxKind.QuestionQuestionToken].includes(node.operatorToken.kind)) collectExpression(node.right)
    }
  }
  // 遍历展示节点与用户可见属性，避免只依赖汉字正则而漏掉硬编码英文。
  function visit(node) {
    if (ts.isJsxText(node) && node.text.trim()) values.push(node.text.trim())
    if (ts.isJsxAttribute(node) && visibleAttributes.has(node.name.getText(source)) && node.initializer) collectExpression(node.initializer)
    if (ts.isJsxExpression(node) && node.expression && !ts.isJsxAttribute(node.parent)) collectExpression(node.expression)
    ts.forEachChild(node, visit)
  }
  visit(source)
  return values
}

// 验证扫描器能发现两种语言的正文、属性、条件分支和模板文本，避免“零发现”来自扫描失效。
test('finds untranslated JSX text and accessible attributes in both languages', () => {
  const source = ts.createSourceFile('fixture.tsx', '<p title="帮助" aria-label={"Help"}>{ready ? "完成" : "Loading"}{`Pending ${count}`}正文</p>', ts.ScriptTarget.Latest, true)
  assert.deepEqual(visibleLiterals(source), ['帮助', 'Help', '完成', 'Loading', 'Pending', '正文'])
})

// 全量扫描正式组件，任何新增普通静态文案都必须改为翻译资源；允许项只保留明确确认的术语。
test('keeps production JSX literals limited to classified terms and symbols', () => {
  const unexpected = []
  const files = sourceFiles(sourceRoot)
  for (const file of files) {
    const relative = path.relative(sourceRoot, file)
    if (/\.test\.tsx?$/.test(relative) || relative.startsWith('test/') || relative.startsWith('i18n/')) continue
    const source = ts.createSourceFile(file, readFileSync(file, 'utf8'), ts.ScriptTarget.Latest, true)
    for (const value of visibleLiterals(source)) {
      if (!retainedText.has(value)) unexpected.push({ file: relative, value })
    }
  }
  assert.deepEqual(unexpected, [])
})
