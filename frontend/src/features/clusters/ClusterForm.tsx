import { useTranslation } from 'react-i18next'
import { localizeApiError } from '../../api/errorMessages'
import { useId, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { ApiError } from '../../api/client'
import type { ClusterDto, ClusterInput, ConnectionTestResult, SecurityProtocol } from '../../api/types'

const secrets = [
  { key: 'password', clear: 'clearPassword', label: '密码' },
  { key: 'caPem', clear: 'clearCaPem', label: 'CA PEM' },
  { key: 'clientCertificatePem', clear: 'clearClientCertificatePem', label: '客户端证书 PEM' },
  { key: 'clientKeyPem', clear: 'clearClientKeyPem', label: '客户端私钥 PEM' },
  { key: 'clientKeyPassphrase', clear: 'clearClientKeyPassphrase', label: '客户端私钥口令' },
] as const

// 在集群表单或规则请求失败时展示统一安全错误、字段提示与请求编号。
export function ApiErrorNotice({ error, fieldIdPrefix }: { error: unknown; fieldIdPrefix?: string }) {
  const { t } = useTranslation()
  if (!error) return null
  // 显示前统一翻译并约束字段路径，防止后端原文经文本或辅助说明关联进入页面。
  const localized = localizeApiError(error, t)
  return <div role="alert"><p>{localized.message}</p>
    {Object.entries(localized.fields).map(([field, message]) => <p key={field} id={fieldIdPrefix ? `${fieldIdPrefix}-${field}` : undefined}>{message}</p>)}
    {localized.requestId && <p>Request ID: {localized.requestId}</p>}
  </div>
}

export interface ClusterFormProps {
  cluster?: ClusterDto
  onSave: (value: ClusterInput) => void
  onTest: (value: ClusterInput) => Promise<ConnectionTestResult>
  onCancel?: () => void
  saving?: boolean
  error?: unknown
}

// 在新增或编辑集群弹窗打开时维护连接草稿，区分保留凭据、显式清除和本次输入。
export function ClusterForm({ cluster, onSave, onTest, onCancel, saving = false, error: saveError }: ClusterFormProps) {
  const id = useId()
  const [value, setValue] = useState({
    name: cluster?.name ?? '', bootstrapServers: cluster?.bootstrapServers ?? '',
    securityProtocol: cluster?.securityProtocol ?? 'PLAINTEXT' as SecurityProtocol,
    saslMechanism: cluster?.saslMechanism ?? '', username: cluster?.username ?? '',
    password: '', caPem: '', clientCertificatePem: '', clientKeyPem: '', clientKeyPassphrase: '',
    connectionTimeoutMs: String(cluster?.connectionTimeoutMs ?? 10000),
    defaultNormalLagMax: String(cluster?.defaultNormalLagMax ?? 999),
    defaultHighLagMin: String(cluster?.defaultHighLagMin ?? 5000),
    defaultOverLimitSeconds: String(cluster?.defaultOverLimitSeconds ?? 30),
  })
  const [cleared, setCleared] = useState<Set<string>>(new Set())
  const test = useMutation({ mutationKey: ['cluster-settings'], mutationFn: onTest })
  const error = test.error ?? saveError
  const busy = saving || test.isPending
  // 在用户编辑字段时清除旧连接测试结果并更新对应草稿值。
  const update = (key: keyof typeof value, next: string) => { test.reset(); setValue(previous => ({ ...previous, [key]: next })) }
  const numeric = ['connectionTimeoutMs', 'defaultNormalLagMax', 'defaultHighLagMin', 'defaultOverLimitSeconds'] as const
  const valid = !!value.name.trim() && !!value.bootstrapServers.trim()
    && numeric.every(key => value[key] !== '' && Number.isSafeInteger(Number(value[key])) && Number(value[key]) >= 0)
    && Number(value.connectionTimeoutMs) > 0 && Number(value.defaultHighLagMin) > Number(value.defaultNormalLagMax)
  // 在表单渲染时生成带禁用状态和服务端错误关联的通用字段。
  const field = (key: keyof typeof value, label: string, type = 'text') => <label className={key === 'bootstrapServers' ? 'field full' : 'field'} htmlFor={`${id}-${key}`}>{label}
    <input id={`${id}-${key}`} type={type} autoComplete={type === 'password' ? 'new-password' : 'off'} disabled={cleared.has(key)}
      value={value[key]} min={type === 'number' ? 0 : undefined} step={type === 'number' ? 1 : undefined}
      aria-invalid={error instanceof ApiError && !!error.fields[key]}
      aria-describedby={error instanceof ApiError && error.fields[key] ? `${id}-error-${key}` : undefined}
      onChange={/* 在字段编辑时更新对应连接草稿并清除过时的连接测试反馈。 */ event => update(key, event.target.value)} />
  </label>

  // 在保存或测试前组装当前配置，仅发送新输入的秘密或显式清除标志。
  const input = (): ClusterInput => {
    const input: ClusterInput = { name: value.name, bootstrapServers: value.bootstrapServers,
      securityProtocol: value.securityProtocol, saslMechanism: value.saslMechanism || null, username: value.username || null,
      connectionTimeoutMs: Number(value.connectionTimeoutMs), defaultNormalLagMax: Number(value.defaultNormalLagMax),
      defaultHighLagMin: Number(value.defaultHighLagMin), defaultOverLimitSeconds: Number(value.defaultOverLimitSeconds) }
    for (const { key, clear } of secrets) {
      if (cleared.has(key)) input[clear] = true
      else if (value[key] !== '') input[key] = value[key]
    }
    return input
  }
  return <form className="cluster-form" onSubmit={/* 在表单提交时，提交当前受控草稿，由父级统一管理保存与错误反馈。 */ event => {
    event.preventDefault()
    if (!valid || busy) return
    test.reset()
    onSave(input())
  }}>
    <fieldset className="form-grid" disabled={busy}><legend className="sr-only">{cluster ? '编辑集群' : '新增集群'}</legend>
      {field('name', '集群名称')}
      <label htmlFor={`${id}-protocol`}>安全协议
        <select id={`${id}-protocol`} value={value.securityProtocol}
          aria-invalid={error instanceof ApiError && !!error.fields.securityProtocol}
          aria-describedby={error instanceof ApiError && error.fields.securityProtocol ? `${id}-error-securityProtocol` : undefined}
          onChange={/* 在字段编辑时更新对应连接草稿并清除过时的连接测试反馈。 */ event => update('securityProtocol', event.target.value)}>
          {['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'].map(protocol => <option key={protocol}>{protocol}</option>)}
        </select>
      </label>
      {field('bootstrapServers', 'Bootstrap Servers')}
      {field('saslMechanism', 'SASL Mechanism')}{field('username', '用户名')}
      {secrets.map(({ key, label }) => <div className="secret-field" key={key}>
        {key === 'password' || key === 'clientKeyPassphrase' ? field(key, label, 'password') : <label htmlFor={`${id}-${key}`}>
        {label}
        <textarea id={`${id}-${key}`} value={value[key]} autoComplete="off" disabled={cleared.has(key)}
          aria-invalid={error instanceof ApiError && !!error.fields[key]}
          aria-describedby={error instanceof ApiError && error.fields[key] ? `${id}-error-${key}` : undefined}
          onChange={/* 在字段编辑时更新对应连接草稿并清除过时的连接测试反馈。 */ event => update(key, event.target.value)} />
      </label>}
        {cluster && <>
          <span>{cluster[`${key}Configured`] ? '已配置' : '未配置'}</span>
          <label><input type="checkbox" checked={cleared.has(key)} onChange={/* 在清除凭据复选框变化时更新显式删除集合，保存前不修改服务端凭据。 */ event => {
            test.reset()
            const next = new Set(cleared)
            if (event.target.checked) next.add(key); else next.delete(key)
            setCleared(next)
          }} />清除已保存的{label}</label>
        </>}
      </div>)}
      {cluster && <p className="form-note full">密码、证书、私钥与口令不回显；留空保留已保存的敏感字段，勾选清除后在保存时删除。</p>}
      {field('connectionTimeoutMs', '连接超时（毫秒）', 'number')}
      {field('defaultNormalLagMax', '默认正常上限', 'number')}
      {field('defaultHighLagMin', '默认高 Lag 起始值', 'number')}
      {field('defaultOverLimitSeconds', '默认持续超限时间（秒）', 'number')}
      <div className="modal-actions full"><button type="button" disabled={!valid || busy} onClick={/* 在测试当前配置按钮点击时从草稿组装输入，测试结果不持久化连接。 */ () => test.mutate(input())}>测试当前配置</button>
      <div>{onCancel && <button type="button" onClick={onCancel}>取消</button>}<button type="submit" disabled={!valid || busy}>保存集群</button></div></div>
    </fieldset>
    {test.isPending && <p role="status">正在测试当前配置…</p>}
    {test.isSuccess && <p role="status">当前配置连接成功：{test.data.brokerCount} Broker，{test.data.topicCount} Topic，耗时 {test.data.elapsedMs} ms</p>}
    <ApiErrorNotice error={error} fieldIdPrefix={`${id}-error`} />
  </form>
}
