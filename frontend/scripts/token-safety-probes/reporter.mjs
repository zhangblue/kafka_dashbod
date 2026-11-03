import { writeFileSync } from 'node:fs'

// 在安全探针报告生成时递归收集真实步骤诊断，供外层扫描敏感值泄露。
function serializeStep(step) {
  return { title: step.title, params: step.params, error: step.error, steps: step.steps.map(serializeStep) }
}

export default class SafetyReporter {
  results = []
  // 在每个探针结束时记录步骤、错误与附件元数据，供真实 runner 安全回归核验。
  onTestEnd(test, result) {
    this.results.push({ title: test.title, status: result.status, errors: result.errors,
      steps: result.steps.map(serializeStep), annotations: test.annotations, attachments: result.attachments })
  }
  // 在探针运行结束时把收集结果写入临时报告，供外层扫描而非输出原始诊断。
  onEnd() { writeFileSync(process.env.TOKEN_SAFETY_STEPS, JSON.stringify(this.results)) }
}
