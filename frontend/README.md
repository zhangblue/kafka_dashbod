# Kafka Lens frontend

React + TypeScript application built with Vite.

```sh
npm ci --legacy-peer-deps
npm run dev
npm run test -- --run
npm run build
```

The initial shell renders the dashboard title and navigation. Backend health is
available at `http://127.0.0.1:3000/api/health` after running
`cargo run -p backend --bin kafka-dashboard` from the repository root.
