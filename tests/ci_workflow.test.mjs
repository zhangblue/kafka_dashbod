import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test('Kafka connection CI installs the curl headers required by bundled librdkafka', async () => {
  const workflow = await readFile(path.join(root, '.github/workflows/kafka-connection.yml'), 'utf8');

  assert.match(
    workflow,
    /apt-get install[^\n]*\blibcurl4-openssl-dev\b/,
    'rdkafka-sys 4.10.0 requires curl/curl.h while compiling bundled librdkafka',
  );
});
