import { defineConfig } from 'tsup'

export default defineConfig({
  entry: [
    'index.ts',
    'lib/tests/consumer.ts',
    'lib/tests/producer.ts',
    'lib/tests/broadcast-consumer.ts',
    'lib/tests/broadcast-producer.ts',
    'lib/tests/mixed-consumer.ts',
    'lib/tests/mixed-producer.ts',
  ],
  format: ['esm'],
  dts: true,
  clean: true,
  sourcemap: true,
})
