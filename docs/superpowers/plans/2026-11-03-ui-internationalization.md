# Kafka Lens 中英文国际化实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 为 Kafka Lens 全部正式页面、弹窗、辅助文本和安全错误增加中文 / English 即时切换，并在首次访问时跟随浏览器、主动切换后持久化选择。

**架构：** 使用 `i18next` 与 `react-i18next` 提供单例翻译运行时，中文和英文资源随前端 bundle 打包；公共语言解析模块负责浏览器检测、`localStorage` 持久化和 `<html lang>` 同步。UI 仅消费翻译键，错误展示按稳定 API code 与受控字段校验类别本地化，语言重渲染不得改变路由、Kafka 会话或页面内存状态。

**技术栈：** React 19、TypeScript 6、Vite 8、i18next、react-i18next、Vitest、Testing Library、Playwright。

---

## 文件结构与职责

**新建：**

- `frontend/src/i18n/language.ts`：支持语言类型、浏览器检测、持久化和文档语言同步。
- `frontend/src/i18n/resources/zh-CN.ts`：中文翻译资源，按 `common/navigation/overview/topics/consumerGroups/clusters/lagRules/feishuAlerts/topicAdmin/apiErrors/validation` 分区。
- `frontend/src/i18n/resources/en.ts`：与中文完全同构的英文资源。
- `frontend/src/i18n/i18next.d.ts`：扩展 i18next 类型，使翻译键受 TypeScript 资源结构约束。
- `frontend/src/i18n/index.ts`：初始化 i18next、注册 React 插件并暴露切换入口。
- `frontend/src/i18n/language.test.ts`：语言解析、存储失败与文档同步测试。
- `frontend/src/i18n/resources.test.ts`：两种资源键集合一致性和禁用空翻译测试。
- `frontend/src/i18n/format.ts`：按当前语言格式化日期、时间与数字。
- `frontend/src/i18n/format.test.ts`：locale 格式化测试。
- `frontend/src/components/LanguageSwitch.tsx`：已确认的顶部栏“中文 / EN”切换控件。
- `frontend/src/components/LanguageSwitch.test.tsx`：选中状态、键盘访问和持久化测试。
- `frontend/src/api/errorMessages.ts`：API code、字段校验类别到翻译键的安全映射。
- `frontend/src/api/errorMessages.test.ts`：已知、未知和字段错误本地化测试。

**修改：**

- `frontend/package.json`、`frontend/package-lock.json`：加入 `i18next` 与 `react-i18next` 锁定依赖。
- `frontend/src/main.tsx`、`frontend/src/test/setup.ts`：加载 i18n 单例；测试默认固定中文并清理语言存储。
- `frontend/src/app/App.tsx`、`frontend/src/features/dashboard/DashboardLayout.tsx`、`frontend/src/features/clusters/ClustersPage.tsx`、`frontend/src/index.css`：导航、顶部栏、语言控件及响应式样式。
- `frontend/src/components/*.tsx`：公共状态、陈旧提示、弹窗和字段说明文案。
- `frontend/src/features/overview/*`、`frontend/src/features/charts/*`：概览与图表文案、时间和数字格式。
- `frontend/src/features/topics/*`：Topics 页面、三个管理弹窗、配置定义与校验文案。
- `frontend/src/features/consumer-groups/*`：消费组页面、历史、状态标签与趋势文本。
- `frontend/src/features/clusters/*`、`frontend/src/features/lag-rules/*`、`frontend/src/features/feishu-alerts/*`：设置页、连接表单、Lag 规则和飞书告警。
- 对应 `*.test.ts(x)`：保留中文回归并新增关键 English 断言。
- `frontend/playwright.config.ts`、`frontend/e2e/kafka-dashboard.spec.ts`：固定现有 E2E 初始中文，并增加不重建会话的切换验收。
- `README.md`：中文说明首次跟随浏览器、顶部栏切换和本地持久化行为。

## 全局约束

- 所有新增具名函数、组件和 Hook 按 `AGENTS.md` 添加中文注释，说明职责与调用时机；语言切换、持久化和会话连续性的关键调用说明原因与顺序。
- `Topic`、`Consumer Group`、`Lag`、`Offset`、`Partition`、`Broker`、`Bootstrap Servers`、`SASL`、`SSL`、`PEM` 与 `Request ID` 保持英文。
- 不翻译品牌、集群名、Topic 名、Group ID、用户输入、Kafka 返回值或配置原值。
- 不新增后端接口或数据库字段，不发送翻译资源请求。
- 不直接展示后端 `message` 或未知字段错误原文；未知错误必须使用当前语言的固定安全提示。
- 语言切换不得刷新页面、重建 `QueryClient`、关闭或创建 Kafka session、重置刷新频率、表单草稿、弹窗、Lag 连续计时或一小时内存历史。
- 现有业务测试默认中文；只有针对性用例切换 English，且用例结束后恢复中文，避免跨测试污染。

### 任务 1：建立类型化 i18n 运行时与语言解析

**文件：**
- 修改：`frontend/package.json`
- 修改：`frontend/package-lock.json`
- 创建：`frontend/src/i18n/language.ts`
- 创建：`frontend/src/i18n/language.test.ts`
- 创建：`frontend/src/i18n/resources/zh-CN.ts`
- 创建：`frontend/src/i18n/resources/en.ts`
- 创建：`frontend/src/i18n/resources.test.ts`
- 创建：`frontend/src/i18n/i18next.d.ts`
- 创建：`frontend/src/i18n/index.ts`
- 修改：`frontend/src/main.tsx`
- 修改：`frontend/src/test/setup.ts`

- [ ] **步骤 1：安装锁定依赖**

运行：

```bash
npm --prefix frontend install i18next react-i18next
```

检查 `frontend/package.json` 与 lockfile 只增加这两个运行依赖及其必要传递依赖，不升级无关包。

- [ ] **步骤 2：先写语言解析失败测试**

在 `language.test.ts` 覆盖：

```ts
expect(resolveInitialLanguage({ stored: null, browserLanguages: ['zh-HK'] })).toBe('zh-CN')
expect(resolveInitialLanguage({ stored: null, browserLanguages: ['en-US', 'zh-CN'] })).toBe('en')
expect(resolveInitialLanguage({ stored: 'en', browserLanguages: ['zh-CN'] })).toBe('en')
expect(resolveInitialLanguage({ stored: 'broken', browserLanguages: [] })).toBe('zh-CN')
```

另用会抛异常的 Storage 替身验证读取、写入失败不阻止当前语言切换，并断言 `applyDocumentLanguage('en')` 设置 `document.documentElement.lang === 'en'`。

- [ ] **步骤 3：运行测试确认失败**

运行：

```bash
npm --prefix frontend run test -- --run src/i18n/language.test.ts
```

预期：FAIL，模块 `./language` 尚不存在。

- [ ] **步骤 4：实现最小语言模块**

实现固定接口：

```ts
export type AppLanguage = 'zh-CN' | 'en'
export const LANGUAGE_STORAGE_KEY = 'kafka-lens.language'
export function resolveInitialLanguage(input: { stored: string | null; browserLanguages: readonly string[] }): AppLanguage
export function readStoredLanguage(storage?: Storage): AppLanguage | null
export function persistLanguage(language: AppLanguage, storage?: Storage): void
export function applyDocumentLanguage(language: AppLanguage): void
```

浏览器语言只判断首个有效首选项：以 `zh` 开头映射 `zh-CN`，其余有效语言映射 `en`；没有有效值回退中文。所有 Storage 访问包在安全边界内，不能输出存储内容。

- [ ] **步骤 5：先写资源完整性失败测试**

递归展开资源对象的叶子路径并断言：

```ts
expect(flattenKeys(en)).toEqual(flattenKeys(zhCN))
expect(Object.values(flattenLeaves(en))).not.toContain('')
expect(Object.values(flattenLeaves(zhCN))).not.toContain('')
```

首批资源必须包含 `common.language.chinese`、`common.language.english`、`common.errors.requestFailed`、`navigation.overview`、`navigation.topics`、`navigation.consumerGroups`、`navigation.settings`。

- [ ] **步骤 6：运行资源测试确认失败**

运行：

```bash
npm --prefix frontend run test -- --run src/i18n/resources.test.ts
```

预期：FAIL，资源文件或要求的翻译键不存在。

- [ ] **步骤 7：实现资源与 i18next 初始化**

以中文对象为结构源，并让英文资源通过 TypeScript 的 `satisfies` 保证同构：

```ts
export const zhCN = {
  common: { language: { chinese: '中文', english: 'English' }, errors: { requestFailed: '请求失败，请稍后重试。' } },
  navigation: { overview: '概览', topics: 'Topics', consumerGroups: 'Consumer Groups', settings: '集群设置' },
} as const

type TranslationShape<T> = { [K in keyof T]: T[K] extends string ? string : TranslationShape<T[K]> }

export const en = {
  common: { language: { chinese: 'Chinese', english: 'English' }, errors: { requestFailed: 'Request failed. Please try again later.' } },
  navigation: { overview: 'Overview', topics: 'Topics', consumerGroups: 'Consumer Groups', settings: 'Cluster Settings' },
} satisfies TranslationShape<typeof zhCN>
```

`i18next.d.ts` 用 `CustomTypeOptions` 把默认 namespace 的 resources 指向 `typeof zhCN`。`index.ts` 使用 `initReactI18next`、内置资源、`fallbackLng: 'zh-CN'`、`supportedLngs: ['zh-CN', 'en']`、同步内置资源初始化和 `interpolation.escapeValue: false`；初始化语言来自安全的存储读取与浏览器语言解析。初始化完成及每次 `languageChanged` 时同步 `<html lang>`。

- [ ] **步骤 8：固定测试语言并验证通过**

`test/setup.ts` 导入 i18n，在每个测试前清除 `LANGUAGE_STORAGE_KEY` 并切到 `zh-CN`；不得清除测试自行使用的其他 localStorage key。运行：

```bash
npm --prefix frontend run test -- --run src/i18n/language.test.ts src/i18n/resources.test.ts src/app/App.test.tsx
npm --prefix frontend run build
```

预期：全部 PASS，生产构建成功。

- [ ] **步骤 9：提交运行时基础**

```bash
git add frontend/package.json frontend/package-lock.json frontend/src/i18n frontend/src/main.tsx frontend/src/test/setup.ts
git commit -m "feat: 建立中英文翻译运行时"
```

### 任务 2：实现顶部栏语言切换并保持应用状态

**文件：**
- 创建：`frontend/src/components/LanguageSwitch.tsx`
- 创建：`frontend/src/components/LanguageSwitch.test.tsx`
- 修改：`frontend/src/app/App.tsx`
- 修改：`frontend/src/app/App.test.tsx`
- 修改：`frontend/src/features/dashboard/DashboardLayout.tsx`
- 修改：`frontend/src/features/dashboard/DashboardLayout.test.tsx`
- 修改：`frontend/src/features/clusters/ClustersPage.tsx`
- 修改：`frontend/src/features/clusters/ClustersPage.test.tsx`
- 修改：`frontend/src/i18n/resources/zh-CN.ts`
- 修改：`frontend/src/i18n/resources/en.ts`
- 修改：`frontend/src/index.css`

- [ ] **步骤 1：先写语言控件失败测试**

验证中文初始状态、English 切换与持久化：

```tsx
render(<LanguageSwitch />)
expect(screen.getByRole('button', { name: '中文' })).toHaveAttribute('aria-pressed', 'true')
await user.click(screen.getByRole('button', { name: 'English' }))
expect(i18n.resolvedLanguage).toBe('en')
expect(localStorage.getItem(LANGUAGE_STORAGE_KEY)).toBe('en')
expect(document.documentElement.lang).toBe('en')
```

再验证两个按钮可由键盘聚焦并激活，存储写入抛错时当前页面仍切换。

- [ ] **步骤 2：运行控件测试确认失败**

运行：

```bash
npm --prefix frontend run test -- --run src/components/LanguageSwitch.test.tsx
```

预期：FAIL，组件尚不存在。

- [ ] **步骤 3：实现已确认方案 A**

`LanguageSwitch` 渲染两个原生按钮，使用 `aria-pressed` 表示当前语言。点击顺序必须先调用 `i18n.changeLanguage(language)`，成功更新内存语言后再尝试持久化；持久化失败不得回滚语言。桌面文案为“中文 / EN”，窄屏通过 CSS 隐藏长标签但保持完整可访问名称。

- [ ] **步骤 4：先写导航和会话连续性失败测试**

在 `App.test.tsx` 增加 English 导航断言，并在已有真实 App 路由测试中记录 `sessionsApi.create` 调用数：

```tsx
const createsBefore = vi.mocked(sessionsApi.create).mock.calls.length
await user.click(screen.getByRole('button', { name: 'English' }))
expect(screen.getByRole('link', { name: 'Overview' })).toBeVisible()
expect(vi.mocked(sessionsApi.create)).toHaveBeenCalledTimes(createsBefore)
```

保持当前路由，并在打开设置弹窗后切换语言，断言弹窗仍打开且已输入字段值不变。

- [ ] **步骤 5：运行页面测试确认失败**

运行：

```bash
npm --prefix frontend run test -- --run src/app/App.test.tsx src/features/dashboard/DashboardLayout.test.tsx src/features/clusters/ClustersPage.test.tsx
```

预期：FAIL，导航与顶部栏仍为硬编码文案，设置顶部栏没有语言入口。

- [ ] **步骤 6：迁移应用外壳与顶部栏**

将 `App` 导航、品牌副标题、侧栏说明改为翻译键；在监控顶部栏刷新按钮之后放置 `LanguageSwitch`；集群设置顶部栏最右侧放置同一组件。状态标签、刷新单位、更新时间与连接状态全部使用翻译键。语言组件必须保持在现有 React 树中，不能用 `key={language}` 包裹 App、Router、DashboardLayout 或 Monitor。

- [ ] **步骤 7：实现响应式样式并验证**

新增 `.language-switch`、`.language-option` 样式，沿用 `--line`、`#22304a` 与 8px 圆角；在 `max-width: 700px` 时使用短可见标签，控件仍位于顶部栏且不覆盖集群选择。运行：

```bash
npm --prefix frontend run test -- --run src/components/LanguageSwitch.test.tsx src/app/App.test.tsx src/features/dashboard/DashboardLayout.test.tsx src/features/clusters/ClustersPage.test.tsx
npm --prefix frontend run build
```

预期：全部 PASS，构建成功。

- [ ] **步骤 8：提交语言入口**

```bash
git add frontend/src/components/LanguageSwitch.tsx frontend/src/components/LanguageSwitch.test.tsx frontend/src/app frontend/src/features/dashboard/DashboardLayout.tsx frontend/src/features/dashboard/DashboardLayout.test.tsx frontend/src/features/clusters/ClustersPage.tsx frontend/src/features/clusters/ClustersPage.test.tsx frontend/src/i18n/resources frontend/src/index.css
git commit -m "feat: 添加顶部栏语言切换"
```

### 任务 3：统一格式化、公共组件和安全错误翻译

**文件：**
- 创建：`frontend/src/i18n/format.ts`
- 创建：`frontend/src/i18n/format.test.ts`
- 创建：`frontend/src/api/errorMessages.ts`
- 创建：`frontend/src/api/errorMessages.test.ts`
- 修改：`frontend/src/api/client.ts`
- 修改：`frontend/src/api/client.test.ts`
- 修改：`frontend/src/components/MetricHelp.tsx`
- 修改：`frontend/src/components/Modal.tsx`
- 修改：`frontend/src/components/StaleNotice.tsx`
- 修改：`frontend/src/components/StatusBadge.tsx`
- 修改：`frontend/src/features/clusters/ClusterForm.tsx`
- 修改：`frontend/src/features/consumer-groups/status.ts`
- 修改：`frontend/src/features/consumer-groups/status.test.ts`
- 修改：`frontend/src/i18n/resources/zh-CN.ts`
- 修改：`frontend/src/i18n/resources/en.ts`

- [ ] **步骤 1：先写 locale 格式失败测试**

固定输入验证 locale 被显式传递：

```ts
expect(formatNumber(1234567, 'en')).toBe('1,234,567')
expect(formatNumber(1234567, 'zh-CN')).toBe('1,234,567')
expect(formatTime(Date.UTC(2026, 0, 2, 3, 4, 5), 'en', 'UTC')).toMatch(/3:04:05/)
expect(formatDateTime(Date.UTC(2026, 0, 2, 3, 4, 5), 'zh-CN', 'UTC')).toContain('2026')
```

`format.ts` 必须接收显式 `AppLanguage`，不得依赖测试机器 locale。

- [ ] **步骤 2：先写错误本地化失败测试**

覆盖下列 code：`VALIDATION_ERROR`、`CLUSTER_NOT_FOUND`、`CONFLICT`、`PAYLOAD_TOO_LARGE`、`INTERNAL_ERROR`、`SESSION_EXPIRED`、`SHUTTING_DOWN`、`SNAPSHOT_IN_PROGRESS`、`KAFKA_AUTH_FAILED`、`KAFKA_DNS_FAILED`、`KAFKA_TIMEOUT`、`KAFKA_TLS_FAILED`、`KAFKA_QUERY_FAILED`、`KAFKA_UNAVAILABLE`、`FEISHU_SEND_FAILED`、`TOPIC_ALREADY_EXISTS`、`TOPIC_NOT_FOUND`、`TOPIC_AUTHORIZATION_FAILED`、`TOPIC_CONFIG_CHANGED`、`TOPIC_OPERATION_TIMEOUT`、`INVALID_TOPIC_CONFIG`、`INTERNAL_TOPIC_PROTECTED`。

核心断言：

```ts
expect(localizeApiError(new ApiError(404, { code: 'CLUSTER_NOT_FOUND', message: 'Cluster not found' }), tEn).message)
  .toBe('Cluster not found.')
expect(localizeApiError(new ApiError(500, { code: 'UNKNOWN', message: '不得显示' }), tEn).message)
  .toBe('Request failed. Please try again later.')
expect(localizeFieldError('unrecognized backend detail', tZh)).toBe('字段值无效。')
```

已知字段类别至少覆盖 required、non-negative integer、positive integer、duplicate pair、invalid security combination、invalid Topic config option、body invalid。

- [ ] **步骤 3：运行新增测试确认失败**

运行：

```bash
npm --prefix frontend run test -- --run src/i18n/format.test.ts src/api/errorMessages.test.ts
```

预期：FAIL，格式与错误本地化模块尚不存在。

- [ ] **步骤 4：实现格式化和安全错误映射**

实现：

```ts
export function formatNumber(value: number, language: AppLanguage): string
export function formatTime(value: number | Date, language: AppLanguage, timeZone?: string): string
export function formatDateTime(value: number | Date, language: AppLanguage, timeZone?: string): string
export function localizeApiError(error: unknown, t: TFunction): { message: string; fields: Record<string, string>; requestId?: string }
export function localizeFieldError(message: string, t: TFunction): string
```

映射表只包含固定 code 和受控服务端校验类别；未知内容绝不插值。`ApiError` 默认 message 改为固定内部占位，不再承担 UI 中文回退。业务分支继续读取 `status` 与 `code`。

- [ ] **步骤 5：迁移公共组件与状态模型**

`ApiErrorNotice` 调用 `localizeApiError`；`MetricHelp`、`Modal`、`StaleNotice` 和 `StatusBadge` 使用翻译键。把 `statusLabels` 改为 `statusTranslationKeys: Record<LagStatus, string>`，由 `StatusBadge` 调用 `t`，纯状态计算不依赖当前语言。时间由 format helper 输出，保留 `dateTime` 原始 ISO 值。

- [ ] **步骤 6：验证公共回归**

运行：

```bash
npm --prefix frontend run test -- --run src/i18n/format.test.ts src/api/client.test.ts src/api/errorMessages.test.ts src/features/consumer-groups/status.test.ts src/features/clusters/ClustersPage.test.tsx
npm --prefix frontend run build
```

预期：全部 PASS；未知错误测试证明后端原文没有进入 DOM。

- [ ] **步骤 7：提交公共本地化能力**

```bash
git add frontend/src/i18n frontend/src/api frontend/src/components frontend/src/features/clusters/ClusterForm.tsx frontend/src/features/consumer-groups/status.ts frontend/src/features/consumer-groups/status.test.ts
git commit -m "feat: 本地化公共状态与安全错误"
```

### 任务 4：迁移概览与图表

**文件：**
- 修改：`frontend/src/features/overview/OverviewPage.tsx`
- 修改：`frontend/src/features/overview/OverviewPage.test.tsx`
- 修改：`frontend/src/features/charts/LagLineChart.tsx`
- 修改：`frontend/src/features/charts/LagLineChart.test.tsx`
- 修改：`frontend/src/i18n/resources/zh-CN.ts`
- 修改：`frontend/src/i18n/resources/en.ts`

- [ ] **步骤 1：先写 English 页面失败测试**

在概览测试中切换 `i18n.changeLanguage('en')` 后断言：`Cluster Overview`、`Live consumption status for {{cluster}}`、`Topics`、`Consumer Groups`、`Total Lag`、`Needs Attention`、`No consumer groups need attention.`。保留集群名和 Topic 名原样。

图表测试断言 `Total Lag · Last Hour`、`Chart data is stale`、`View chart data`、`Collected At`、`Unknown / Incomplete`，并验证系列名中的 Group ID / Topic 未翻译。

- [ ] **步骤 2：运行概览测试确认失败**

运行：

```bash
npm --prefix frontend run test -- --run src/features/overview/OverviewPage.test.tsx src/features/charts/LagLineChart.test.tsx
```

预期：FAIL，页面仍展示中文普通文案。

- [ ] **步骤 3：迁移概览和图表文本**

为指标卡、筛选、Lag 分布、需要关注表、未知值、陈旧状态和图表辅助表格增加 `overview` 翻译键。`LagLineChart` 使用当前语言格式化采集时间和已知数值；未知值使用翻译键。ECharts 实例不因语言变化重建，但设置 effect 依赖语言和翻译结果，从而更新 legend、tooltip 和辅助文案。

- [ ] **步骤 4：验证并提交**

运行：

```bash
npm --prefix frontend run test -- --run src/features/overview/OverviewPage.test.tsx src/features/charts/LagLineChart.test.tsx
npm --prefix frontend run build
```

预期：全部 PASS。提交：

```bash
git add frontend/src/features/overview frontend/src/features/charts frontend/src/i18n/resources
git commit -m "feat: 国际化概览与 Lag 图表"
```

### 任务 5：迁移 Topics 与 Topic 管理弹窗

**文件：**
- 修改：`frontend/src/features/topics/TopicsPage.tsx`
- 修改：`frontend/src/features/topics/TopicsPage.test.tsx`
- 修改：`frontend/src/features/topics/CreateTopicModal.tsx`
- 修改：`frontend/src/features/topics/CreateTopicModal.test.tsx`
- 修改：`frontend/src/features/topics/DeleteTopicModal.tsx`
- 修改：`frontend/src/features/topics/DeleteTopicModal.test.tsx`
- 修改：`frontend/src/features/topics/TopicConfigModal.tsx`
- 修改：`frontend/src/features/topics/TopicConfigModal.test.tsx`
- 修改：`frontend/src/features/topics/topicConfig.ts`
- 修改：`frontend/src/features/topics/topicConfig.test.ts`
- 修改：`frontend/src/i18n/resources/zh-CN.ts`
- 修改：`frontend/src/i18n/resources/en.ts`

- [ ] **步骤 1：先写 Topics English 失败测试**

页面测试覆盖 `Topics`、搜索可访问名称、`Create Topic`、Offset 说明、表头、展开 Partition、部分数据、配置与删除操作。三个弹窗分别验证：

- 创建：标题、Partition 数、副本因子、Broker 默认值、成功/失败状态；
- 删除：不可恢复警告、完整名称确认、内部 Topic 保护；
- 参数：8 个白名单参数的英文 label、description、risk、source、恢复 Broker 默认、冲突和 202 已应用未确认提示。

Topic 名和 8 个配置 key 必须保持原样。

- [ ] **步骤 2：运行目标测试确认失败**

运行：

```bash
npm --prefix frontend run test -- --run src/features/topics
```

预期：FAIL，English 断言找不到。

- [ ] **步骤 3：把配置定义改为翻译键元数据**

`TopicConfigDefinition` 改为保存 `labelKey`、`descriptionKey`、`riskKey`、`unitKey` 和选项 `labelKey`，仍保留 key、inputType 与 value。`validateTopicConfigValue` 返回固定错误键：

```ts
export type TopicConfigValidationKey = 'topicAdmin.validation.unsupported' | 'topicAdmin.validation.invalidOption' | 'topicAdmin.validation.invalidInteger'
export function validateTopicConfigValue(key: string, value: string): TopicConfigValidationKey | undefined
```

组件负责用 `t(validationKey)` 展示，纯校验不读取全局语言。

- [ ] **步骤 4：迁移页面和三个弹窗**

所有普通文案、`aria-label`、`title`、placeholder、状态和确认说明使用 `topics` / `topicAdmin` 翻译键。Kafka 配置来源 `topicOverride`、`brokerDefault`、`unknown` 通过翻译键展示，但提交值不变。动态数字使用插值，Topic 名只作为安全文本变量。

- [ ] **步骤 5：验证并提交**

运行：

```bash
npm --prefix frontend run test -- --run src/features/topics
npm --prefix frontend run build
```

预期：全部 PASS。提交：

```bash
git add frontend/src/features/topics frontend/src/i18n/resources
git commit -m "feat: 国际化 Topic 管理页面"
```

### 任务 6：迁移 Consumer Groups 与 Lag 历史

**文件：**
- 修改：`frontend/src/features/consumer-groups/ConsumerGroupsPage.tsx`
- 修改：`frontend/src/features/consumer-groups/ConsumerGroupsPage.test.tsx`
- 修改：`frontend/src/features/consumer-groups/ConsumerGroupHistory.tsx`
- 修改：`frontend/src/features/consumer-groups/ConsumerGroupLagProvider.tsx`
- 修改：`frontend/src/features/consumer-groups/useConsumerGroupLag.ts`
- 修改：`frontend/src/features/dashboard/useMonitorSession.ts`
- 修改：`frontend/src/features/dashboard/useMonitorSession.test.tsx`
- 修改：`frontend/src/i18n/resources/zh-CN.ts`
- 修改：`frontend/src/i18n/resources/en.ts`

- [ ] **步骤 1：先写 English 消费组失败测试**

覆盖搜索、全部/正常/积压/高 Lag/未提交/采集异常筛选、表头、趋势、未知 Partition、规则来源、展开明细和历史区域。English 文案使用 `Backlog`、`High Lag`、`Uncommitted`、`Collection Error`、`Custom Rule`、`Last Hour Lag History`；Group ID、Topic、Offset 和 Lag 保持原样。

在监控 Hook 测试中验证用户可见的连接失败状态通过资源翻译，但 `ApiError.code` 分支和 session 重试次数不变。

- [ ] **步骤 2：运行目标测试确认失败**

运行：

```bash
npm --prefix frontend run test -- --run src/features/consumer-groups src/features/dashboard/useMonitorSession.test.tsx
```

预期：FAIL，组件仍有硬编码中文文案。

- [ ] **步骤 3：迁移消费组展示层**

页面、历史筛选与所有可访问文本使用 `consumerGroups` 翻译键。趋势内部值继续是 `up/down/flat/null`，展示时翻译。`ConsumerGroupLagProvider` 和 `useConsumerGroupLag` 的状态推进保持纯数据逻辑，不把翻译结果写入连续计时状态。

`useMonitorSession` 不再创建面向用户的中文 `ApiError.message`；发生本地合成错误时使用稳定 code，由公共错误展示层翻译。

- [ ] **步骤 4：验证并提交**

运行：

```bash
npm --prefix frontend run test -- --run src/features/consumer-groups src/features/dashboard/useMonitorSession.test.tsx
npm --prefix frontend run build
```

预期：全部 PASS，Lag 计时和历史回归不变。提交：

```bash
git add frontend/src/features/consumer-groups frontend/src/features/dashboard/useMonitorSession.ts frontend/src/features/dashboard/useMonitorSession.test.tsx frontend/src/i18n/resources
git commit -m "feat: 国际化消费组与 Lag 历史"
```

### 任务 7：迁移集群设置、Lag 规则和飞书告警

**文件：**
- 修改：`frontend/src/features/clusters/ClusterForm.tsx`
- 修改：`frontend/src/features/clusters/ClustersPage.tsx`
- 修改：`frontend/src/features/clusters/ClustersPage.test.tsx`
- 修改：`frontend/src/features/lag-rules/LagRulesForm.tsx`
- 修改：`frontend/src/features/lag-rules/LagRulesForm.test.tsx`
- 修改：`frontend/src/features/feishu-alerts/FeishuAlertForm.tsx`
- 修改：`frontend/src/features/feishu-alerts/FeishuAlertForm.test.tsx`
- 修改：`frontend/src/i18n/resources/zh-CN.ts`
- 修改：`frontend/src/i18n/resources/en.ts`

- [ ] **步骤 1：先写设置页 English 失败测试**

集群设置覆盖页面标题、新增/编辑/删除、连接测试、凭据已配置标志、清除选项、协议字段、默认 Lag 阈值和保存结果。Lag 规则覆盖默认范围、Group/Topic 覆盖、持续超限时间、缺失资源只读与删除。飞书覆盖 Token 显隐、掩码、测试通知、规则新增/删除、Lag 阈值、至少 10 秒校验、保存成功和失败。

用一个状态保持用例：打开飞书弹窗、填写非敏感字段、切换 English，断言弹窗仍打开、规则行值不变且没有新增 session 请求。

- [ ] **步骤 2：运行目标测试确认失败**

运行：

```bash
npm --prefix frontend run test -- --run src/features/clusters src/features/lag-rules src/features/feishu-alerts
```

预期：FAIL，English 文案和状态保持断言不满足。

- [ ] **步骤 3：迁移集群与规则表单**

把 `secrets` 的 label 改为 translation key；普通字段、legend、按钮、说明、成功状态和删除确认全部使用资源。协议名与配置值保持英文。Lag 范围条和规则行的动态字段使用插值，Group/Topic 原样传入。

- [ ] **步骤 4：迁移飞书告警**

Token 不进入翻译插值、测试标题或错误诊断。眼睛按钮可访问名称在两种语言中准确表达显示/隐藏；已配置掩码原样展示。飞书规则的本地校验返回固定错误键，由组件翻译。保存和测试失败优先按 `FEISHU_SEND_FAILED` 等 code 翻译。

- [ ] **步骤 5：验证并提交**

运行：

```bash
npm --prefix frontend run test -- --run src/features/clusters src/features/lag-rules src/features/feishu-alerts
node --test frontend/scripts/token-safety.test.mjs
npm --prefix frontend run build
```

预期：全部 PASS，Token 安全测试 5 项通过。提交：

```bash
git add frontend/src/features/clusters frontend/src/features/lag-rules frontend/src/features/feishu-alerts frontend/src/i18n/resources
git commit -m "feat: 国际化集群与告警配置"
```

### 任务 8：全量文案审计、E2E 与中文文档

**文件：**
- 修改：`frontend/playwright.config.ts`
- 修改：`frontend/e2e/kafka-dashboard.spec.ts`
- 修改：`frontend/src/test/handlers.ts`
- 修改：`README.md`
- 复核：`frontend/src/**/*.ts`
- 复核：`frontend/src/**/*.tsx`

- [ ] **步骤 1：固定现有 E2E 初始语言**

在 Playwright `use` 配置中设置 `locale: 'zh-CN'`，并在每个用例开始前删除 `kafka-lens.language`，保证既有中文定位器不受运行机器语言影响。不得清除测试需要的其他浏览器存储。

- [ ] **步骤 2：先写切换语言 E2E 失败场景**

新增一个不写 Kafka 的浏览器场景：进入监控页，记录当前 session 创建请求数和当前 URL；点击 `English`，断言导航、顶部栏、页面标题和一个已打开弹窗变为英文，URL、表单值和弹窗打开状态不变，且没有第二次 `POST /api/clusters/:id/sessions`。再切回中文并断言选择写入 localStorage。

- [ ] **步骤 3：运行针对性 E2E 或组件替代验证失败**

优先运行隔离脚本：

```bash
npm --prefix frontend run test:e2e
```

预期：新增场景在尚未补齐的文案、持久化或 session 连续性处 FAIL。若本机一次性 Kafka / Docker 不可用，记录明确原因，并先用 `App.test.tsx` 的同等请求计数测试完成红绿循环；不得把未运行的 E2E 报告为通过。

- [ ] **步骤 4：执行全仓前端文案审计**

使用 AST 或 TypeScript scanner 枚举 JSXText、面向用户的字符串属性与错误展示字符串；逐项分类为：翻译键、明确保留的 Kafka 术语/品牌/测试 fixture、中文代码注释。禁止仅用汉字正则作为完成证据。

重点复核：`aria-label`、`title`、placeholder、dialog name、图表 legend、空状态、加载状态、按钮、字段说明、表格标题和本地合成错误。英文资源中不得出现普通中文 UI 文案，中文资源中允许保留已确认技术术语。

- [ ] **步骤 5：更新 README 中文说明**

在“页面视觉基准”后增加“语言切换”小节，说明首次跟随浏览器、顶部栏“中文 / EN”、用户选择仅保存在当前浏览器、清除站点存储后重新跟随浏览器，以及 Kafka 专有术语保持英文。

- [ ] **步骤 6：运行完整前端与发行回归**

运行：

```bash
npm --prefix frontend run test -- --run
npm --prefix frontend run build
node --test frontend/scripts/token-safety.test.mjs
node --test scripts/e2e-preview.test.mjs
node --test tests/frontend_styles.test.mjs tests/release_scripts.test.mjs
git diff --check
```

预期：前端、Token 安全、预览与发行脚本测试全部 PASS；构建只允许既有资源块体积提示。因为新增运行依赖会进入发行包，至少执行一个本机平台构建与验证：

```bash
./tools/build-release.sh --platform darwin-arm64
./tools/verify-release.sh dist/kafka-dashboard-v0.3.0-darwin-arm64
```

预期：发行包页面可加载，语言切换资源已包含在静态 bundle 中；验证过程不在源码目录留下 `.env`、数据库、PID 或日志。

- [ ] **步骤 7：检查秘密与生成文件**

运行：

```bash
git status --short
git diff --check
git grep -n -I -E '(access_token=|hook/[A-Za-z0-9_-]{20,}|BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY)' -- ':!packaging/.env.example' ':!frontend/package-lock.json'
git ls-files 'dist/**' 'frontend/dist/**' 'node_modules/**' '.env' '*.db' '*.db-wal' '*.db-shm'
```

预期：只出现已分类的文档扫描表达式，不出现真实秘密；生成物查询无输出。

- [ ] **步骤 8：提交审计与文档**

```bash
git add README.md frontend/playwright.config.ts frontend/e2e/kafka-dashboard.spec.ts frontend/src/test/handlers.ts
git add -u frontend/src
git commit -m "test: 验收中英文页面切换"
```

## 最终验收清单

- 首次访问按浏览器语言选择中文或 English，明确选择优先且可持久化。
- `<html lang>` 与当前语言一致，顶部栏控件在桌面和窄屏均可键盘操作。
- 全部正式页面、弹窗、图表、辅助文本和安全错误可即时切换。
- Kafka 术语、品牌和用户数据保持原样。
- 未知后端消息不会进入 DOM；Request ID 与字段可访问关系保留。
- 语言切换不重建 Kafka session，不改变路由、刷新频率、表单、弹窗、Lag 连续计时或内存历史。
- 中英文资源键集合一致，测试、构建、E2E 与发行验证结果有实际命令证据。
