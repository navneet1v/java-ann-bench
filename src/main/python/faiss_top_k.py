import numpy as np
import faiss

dimensions = 768

train = np.fromfile('train.fvecs', dtype='float32').reshape(-1, dimensions)
test = np.fromfile('test.fvecs', dtype='float32').reshape(-1, dimensions)

index = faiss.IndexFlatL2(dimensions)
index.add(train)

k = 100
D, I = index.search(test[0:10000], k)  # `D` is an array of distances, `I` is an array of indices
I = np.array(I, dtype=np.int32)
I.tofile('neighbors.ivecs')
