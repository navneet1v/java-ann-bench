import numpy as np
import random

from collections import namedtuple
from datasets import load_dataset
from tqdm import tqdm

Dataset = namedtuple('Dataset', ['name', 'size', 'dimensions'])
wiki_en_embeddings = Dataset('Cohere/wikipedia-22-12-en-embeddings', 35_167_920,
                             768)
simple = Dataset('Cohere/wikipedia-22-12-simple-embeddings', 485_859, 768)

dataset_info = wiki_en_embeddings
random.seed(0)

test_indexes = set()
while len(test_indexes) < 10000:
  test_indexes.add(random.randint(0, dataset_info.size - 1))

seen = set()
total = 0

dataset = load_dataset(dataset_info.name, split='train', streaming=True)

with open('train.fvecs', 'wb') as train:
  with open('test.fvecs', 'wb') as test:
    for i, doc in enumerate(tqdm(dataset, total=dataset_info.size)):
      total += 1
      if total % 100000 == 0:
        print(f'at {total}')
      test_embedding = i in test_indexes
      if test_embedding:
        seen.add(i)

      emb = doc['emb']
      emb_array = np.array(emb, dtype='<f4')
      file = test if test_embedding else train
      file.write(emb_array.tobytes())

print(f'added {total} total vectors')
print(f'added {total - len(seen)} train vectors')
print(f'added {len(seen)} test vectors')
