"""
run_training.py
CLI script to train the issue-detection model.

Usage (from dashoard/ folder):
    python -m ml.run_training
or:
    python ml/run_training.py
"""

import os
import sys

# Make sure 'ml' package is importable
HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(HERE)
sys.path.insert(0, BASE)

from ml.trainer import train

CLEAN_DIR = os.path.join(BASE, 'train_datasets')
DIRTY_DIR = os.path.join(BASE, 'mock_databases')

if __name__ == '__main__':
    print('=' * 60)
    print('  DATA QUALITY ML — TRAINING')
    print('=' * 60)
    print(f'  Clean dir : {CLEAN_DIR}')
    print(f'  Dirty dir : {DIRTY_DIR}')
    print('=' * 60)
    print()

    result = train(CLEAN_DIR, DIRTY_DIR, max_rows_per_file=3000, verbose=True)

    print()
    print('=' * 60)
    print('  TRAINING COMPLETE')
    print('=' * 60)
    print(f'  Total cells   : {result["total_cells"]:,}')
    print(f'  Issue cells   : {result["issue_cells"]:,}  ({result["issue_rate"]*100:.1f}%)')
    print(f'  ROC-AUC       : {result["roc_auc"]:.4f}')
    print(f'  Avg Precision : {result["avg_precision"]:.4f}')
    print(f'  Model saved   : {result["model_path"]}')
    print('=' * 60)
