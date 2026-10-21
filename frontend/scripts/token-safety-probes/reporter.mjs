import { writeFileSync } from 'node:fs'

function serializeStep(step) {
  return { title: step.title, params: step.params, error: step.error, steps: step.steps.map(serializeStep) }
}

export default class SafetyReporter {
  results = []
  onTestEnd(test, result) {
    this.results.push({ title: test.title, status: result.status, errors: result.errors,
      steps: result.steps.map(serializeStep), annotations: test.annotations, attachments: result.attachments })
  }
  onEnd() { writeFileSync(process.env.TOKEN_SAFETY_STEPS, JSON.stringify(this.results)) }
}
