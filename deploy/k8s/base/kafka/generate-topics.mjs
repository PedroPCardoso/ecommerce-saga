#!/usr/bin/env node
import { allTopics } from '@ecommerce/contracts';
import { writeFileSync } from 'node:fs';

const manifests = allTopics()
  .map(
    (topic) => `---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: ${topic.replace(/[^a-z0-9-]/gi, '-').toLowerCase()}
  namespace: ecommerce-saga
  labels:
    strimzi.io/cluster: saga-kafka
spec:
  topicName: ${topic}
  partitions: 3
  replicas: 1
`,
  )
  .join('');

writeFileSync(new URL('./topics-generated.yaml', import.meta.url), manifests);
console.log(`Gerados ${allTopics().length} KafkaTopic CRs em topics-generated.yaml`);
