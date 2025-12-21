import os

def get_latest_dataset_file(dir, base, ext):
    prefix = base + "_"
    files = [
        f for f in os.listdir(dir)
        if f.startswith(prefix) and f.endswith(ext)
    ]
    files = sorted(files)   # 今天最大
    return f'{dir}/{files[-1]}' if files else None