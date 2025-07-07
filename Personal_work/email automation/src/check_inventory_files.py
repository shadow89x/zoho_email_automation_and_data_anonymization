import pandas as pd

def check_inventory_files():
    """inventory 파일들의 컬럼 구조 확인"""
    
    files_to_check = [
        'data/inventory.CSV',
        'data/processed_inventory_data_with_item_code.csv',
        'data/inventory_final_cleaned.csv',
        'data/inventory_final_anonymous.csv',
        'data/inventory_final_anonymous_cleaned.csv',
        'data/inventory_final_anonymous_updated.csv'
    ]
    
    for file_path in files_to_check:
        try:
            df = pd.read_csv(file_path, nrows=5)  # 처음 5행만 읽기
            print(f"\n=== {file_path} ===")
            print(f"행 수: {len(pd.read_csv(file_path))}")
            print(f"컬럼: {list(df.columns)}")
            print("샘플 데이터:")
            print(df.head(3))
        except Exception as e:
            print(f"\n=== {file_path} ===")
            print(f"오류: {e}")

if __name__ == "__main__":
    check_inventory_files() 