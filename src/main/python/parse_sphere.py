import random
import json
import numpy as np

from tqdm import tqdm

path = '/Users/kevin.rosendahl/Downloads/sphere.100k.jsonl'
size = 100_000

random.seed(0)

seen = set()
total = 0
test_indexes = set()
while len(test_indexes) < 10000:
  test_indexes.add(random.randint(0, size - 1))

with open('train.fvecs', 'wb') as train:
  with open('test.fvecs', 'wb') as test:
    with open(path) as jsonl_file:
      for i, jsonl in enumerate(tqdm(jsonl_file, total=size)):
        total += 1
        test_embedding = i in test_indexes
        if test_embedding:
          seen.add(i)

        json_parsed = json.loads(jsonl)
        vector = json_parsed['vector']

        emb_array = np.array(vector, dtype='<f4')
        file = test if test_embedding else train
        file.write(emb_array.tobytes())
