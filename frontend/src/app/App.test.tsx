import { render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'
import App from './App'

it('renders the dashboard navigation', () => {
  render(<App />)
  expect(screen.getByText('Kafka Lens')).toBeInTheDocument()
  expect(screen.getByRole('navigation')).toBeInTheDocument()
  expect(screen.getByText('概览')).toBeInTheDocument()
  expect(screen.getByText('Topics')).toBeInTheDocument()
  expect(screen.getByText('Consumer Groups')).toBeInTheDocument()
  expect(screen.getByText('集群设置')).toBeInTheDocument()
})
