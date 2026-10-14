import { useId, useState } from 'react'
import { ApiError } from '../../api/client'
import type { ClusterDto, ClusterInput, SecurityProtocol } from '../../api/types'

export function ApiErrorNotice({ error }: { error: unknown }) {
  if (!error) return null
  return <div role="alert"><p>{error instanceof ApiError ? error.message : '请求失败，请稍后重试'}</p>
    {error instanceof ApiError && <>{Object.entries(error.fields).map(([field, message]) => <p key={field}>{message}</p>)}
      {error.requestId && <p>Request ID: {error.requestId}</p>}</>}
  </div>
}

export interface ClusterFormProps {
  cluster?: ClusterDto
  onSave: (value: ClusterInput) => void
  onCancel?: () => void
  saving?: boolean
  error?: unknown
}

export function ClusterForm({ cluster, onSave, onCancel, saving = false, error }: ClusterFormProps) {
  const id = useId()
  const [value, setValue] = useState({
    name: cluster?.name ?? '', bootstrapServers: cluster?.bootstrapServers ?? '',
    securityProtocol: cluster?.securityProtocol ?? 'PLAINTEXT' as SecurityProtocol,
    saslMechanism: cluster?.saslMechanism ?? '', username: cluster?.username ?? '',
    password: '', caPem: '', clientCertificatePem: '', clientKeyPem: '',
    connectionTimeoutMs: String(cluster?.connectionTimeoutMs ?? 10000),
    defaultNormalLagMax: String(cluster?.defaultNormalLagMax ?? 999),
    defaultHighLagMin: String(cluster?.defaultHighLagMin ?? 5000),
    defaultOverLimitSeconds: String(cluster?.defaultOverLimitSeconds ?? 30),
  })
  const numeric = ['connectionTimeoutMs', 'defaultNormalLagMax', 'defaultHighLagMin', 'defaultOverLimitSeconds'] as const
  const valid = !!value.name.trim() && !!value.bootstrapServers.trim()
    && numeric.every(key => value[key] !== '' && Number.isSafeInteger(Number(value[key])) && Number(value[key]) >= 0)
    && Number(value.connectionTimeoutMs) > 0 && Number(value.defaultHighLagMin) > Number(value.defaultNormalLagMax)
  const field = (key: keyof typeof value, label: string, type = 'text') => <label htmlFor={`${id}-${key}`}>{label}
    <input id={`${id}-${key}`} type={type} autoComplete={key === 'password' ? 'new-password' : 'off'}
      value={value[key]} min={type === 'number' ? 0 : undefined} step={type === 'number' ? 1 : undefined}
      aria-invalid={error instanceof ApiError && !!error.fields[key]}
      onChange={event => setValue({ ...value, [key]: event.target.value })} />
  </label>

  return <form onSubmit={event => {
    event.preventDefault()
    if (!valid || saving) return
    const input: ClusterInput = { name: value.name, bootstrapServers: value.bootstrapServers,
      securityProtocol: value.securityProtocol, saslMechanism: value.saslMechanism || null, username: value.username || null,
      connectionTimeoutMs: Number(value.connectionTimeoutMs), defaultNormalLagMax: Number(value.defaultNormalLagMax),
      defaultHighLagMin: Number(value.defaultHighLagMin), defaultOverLimitSeconds: Number(value.defaultOverLimitSeconds) }
    for (const key of ['password', 'caPem', 'clientCertificatePem', 'clientKeyPem'] as const) {
      if (value[key] !== '') input[key] = value[key]
    }
    onSave(input)
  }}>
    <fieldset disabled={saving}><legend>{cluster ? '编辑集群' : '新增集群'}</legend>
      {field('name', '集群名称')}{field('bootstrapServers', 'Bootstrap Servers')}
      <label htmlFor={`${id}-protocol`}>安全协议</label>
      <select id={`${id}-protocol`} value={value.securityProtocol} onChange={event => setValue({ ...value, securityProtocol: event.target.value as SecurityProtocol })}>
        {['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'].map(protocol => <option key={protocol}>{protocol}</option>)}
      </select>
      {field('saslMechanism', 'SASL Mechanism')}{field('username', '用户名')}{field('password', '密码', 'password')}
      {(['caPem', 'clientCertificatePem', 'clientKeyPem'] as const).map((key, index) => <label key={key} htmlFor={`${id}-${key}`}>
        {['CA PEM', '客户端证书 PEM', '客户端私钥 PEM'][index]}
        <textarea id={`${id}-${key}`} value={value[key]} autoComplete="off" onChange={event => setValue({ ...value, [key]: event.target.value })} />
      </label>)}
      {cluster && <p>密码与证书不回显；留空保留已保存的敏感字段。</p>}
      {field('connectionTimeoutMs', '连接超时（毫秒）', 'number')}
      {field('defaultNormalLagMax', '默认正常上限', 'number')}
      {field('defaultHighLagMin', '默认高 Lag 起始值', 'number')}
      {field('defaultOverLimitSeconds', '默认持续超限时间（秒）', 'number')}
      <button type="submit" disabled={!valid || saving}>保存集群</button>
      {onCancel && <button type="button" onClick={onCancel}>取消</button>}
    </fieldset>
    <ApiErrorNotice error={error} />
  </form>
}
