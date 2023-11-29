from tqdm import tqdm
import os

# offset = 2_000_000
offset = 2_000


def find_offsets(file_path):
  newline_count = 0
  offsets = []
  file_size = os.path.getsize(file_path)

  with open(file_path, 'rb') as file:
    with tqdm(total=file_size, unit='B', unit_scale=True) as pbar:
      while True:
        chunk = file.read(1024 * 1024)  # Read in chunks
        if not chunk:
          break
        for byte in chunk:
          if byte == 10:  # Newline character in ASCII
            newline_count += 1
            if newline_count % offset == 0:
              offsets.append(file.tell() - (1024 * 1024 - chunk.index(byte)))
        pbar.update(len(chunk))
  return offsets

# Use the function
file_path = '/Users/kevin.rosendahl/Downloads/sphere.100k.jsonl'
offsets = find_offsets(file_path)
print(len(offsets))
print(offsets)
