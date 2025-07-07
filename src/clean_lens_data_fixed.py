import pandas as pd
import numpy as np
import re

def find_category_columns(df):
    """
    데이터프레임에서 category 관련 컬럼들을 찾는 함수
    """
    category_columns = [col for col in df.columns if 'category' in col.lower()]
    
    level1_col = None
    level2_col = None
    level3_col = None
    
    for col in category_columns:
        if 'level_1' in col.lower() or 'level1' in col.lower():
            level1_col = col
        elif 'level_2' in col.lower() or 'level2' in col.lower():
            level2_col = col
        elif 'level_3' in col.lower() or 'level3' in col.lower():
            level3_col = col
    
    return level1_col, level2_col, level3_col

def remove_prefix(text):
    """
    첫 번째 띄어쓰기까지의 부분을 제거하는 함수
    """
    if pd.isna(text) or text == '':
        return text
    
    text = str(text).strip()
    # 첫 번째 띄어쓰기 위치 찾기
    space_pos = text.find(' ')
    
    if space_pos == -1:  # 띄어쓰기가 없으면 전체 제거
        return ''
    else:  # 첫 번째 띄어쓰기 이후 부분만 반환
        return text[space_pos + 1:].strip()

def clean_lens_data(input_file, output_file):
    """
    Lens 데이터 정리 함수
    
    Args:
        input_file (str): 입력 CSV 파일 경로
        output_file (str): 출력 CSV 파일 경로
    """
    # 데이터 로드
    print(f"데이터 로드 중: {input_file}")
    inv_df = pd.read_csv(input_file)
    print(f"전체 데이터: {len(inv_df)}행")
    print(f"컬럼명: {list(inv_df.columns)}")
    
    # category 관련 컬럼 찾기
    level1_col, level2_col, level3_col = find_category_columns(inv_df)
    
    print(f"\n찾은 컬럼:")
    print(f"Level 1: {level1_col}")
    print(f"Level 2: {level2_col}")
    print(f"Level 3: {level3_col}")
    
    if not level1_col:
        print("category_level_1 관련 컬럼을 찾을 수 없습니다.")
        print("사용 가능한 컬럼:")
        for col in inv_df.columns:
            print(f"- {col}")
        return None, None
    
    # 1. lens 관련 데이터 분류
    lens_keywords = ['lens', '렌즈', 'lenses']
    
    # 소문자로 변환해서 lens 관련 데이터 찾기
    inv_df['temp_category_lower'] = inv_df[level1_col].astype(str).str.lower()
    
    # lens 관련 데이터 필터링
    lens_mask = inv_df['temp_category_lower'].str.contains('|'.join(lens_keywords), na=False)
    lens_df = inv_df[lens_mask].copy()
    
    print(f"\nLens 관련 데이터: {len(lens_df)}행")
    print(f"{level1_col} 분포:")
    print(inv_df[level1_col].value_counts())
    
    if len(lens_df) == 0:
        print("Lens 관련 데이터가 없습니다.")
        return None, None
    
    # 임시 컬럼 제거
    lens_df = lens_df.drop('temp_category_lower', axis=1)
    
    # 2. category_level_1을 모두 'lens'로 통일
    lens_df[level1_col] = 'lens'
    print(f"\nCategory Level 1을 'lens'로 통일 완료")
    
    # 3. category_level_2와 category_level_3에서 앞부분 제거
    # 원본 백업
    if level2_col:
        lens_df[f'{level2_col}_original'] = lens_df[level2_col].copy()
        lens_df[level2_col] = lens_df[level2_col].apply(remove_prefix)
        print(f"{level2_col} 정리 완료")
    
    if level3_col:
        lens_df[f'{level3_col}_original'] = lens_df[level3_col].copy()
        lens_df[level3_col] = lens_df[level3_col].apply(remove_prefix)
        print(f"{level3_col} 정리 완료")
    
    # 4. 원본 데이터에서 lens 데이터를 정리된 데이터로 교체
    # lens가 아닌 데이터만 필터링
    non_lens_mask = ~inv_df['temp_category_lower'].str.contains('|'.join(lens_keywords), na=False)
    non_lens_df = inv_df[non_lens_mask].copy()
    
    # 임시 컬럼 제거
    non_lens_df = non_lens_df.drop('temp_category_lower', axis=1)
    
    # 정리된 lens 데이터와 합치기
    cleaned_inv_df = pd.concat([non_lens_df, lens_df], ignore_index=True)
    
    print(f"\n최종 데이터: {len(cleaned_inv_df)}행")
    print(f"Lens 데이터: {len(lens_df)}행")
    print(f"Non-lens 데이터: {len(non_lens_df)}행")
    
    # 5. 원본 백업 컬럼 제거
    if level2_col and f'{level2_col}_original' in lens_df.columns:
        lens_df = lens_df.drop(f'{level2_col}_original', axis=1)
        print(f"{level2_col}_original 컬럼 제거 완료")
    
    if level3_col and f'{level3_col}_original' in lens_df.columns:
        lens_df = lens_df.drop(f'{level3_col}_original', axis=1)
        print(f"{level3_col}_original 컬럼 제거 완료")
    
    # 5-1. lens row의 Description, Item 덮어쓰기
    if level1_col and level2_col and level3_col:
        lens_mask = cleaned_inv_df[level1_col] == 'lens'
        def merge_cat2_cat3(row):
            v2 = str(row[level2_col]) if pd.notna(row[level2_col]) else ''
            v3 = str(row[level3_col]) if pd.notna(row[level3_col]) else ''
            merged = (v2 + ' ' + v3).strip()
            return merged
        merged_str = cleaned_inv_df[lens_mask].apply(merge_cat2_cat3, axis=1)
        cleaned_inv_df.loc[lens_mask, 'Description'] = merged_str.values
        cleaned_inv_df.loc[lens_mask, 'Item'] = merged_str.values
        print('lens row의 Description, Item 컬럼 덮어쓰기 완료')
    
    # 6. 결과 저장
    cleaned_inv_df.to_csv(output_file, index=False)
    print(f"\n정리된 데이터 저장 완료: {output_file}")
    
    # lens 데이터만 별도 저장
    lens_output_file = output_file.replace('.csv', '_lens_only.csv')
    lens_df.to_csv(lens_output_file, index=False)
    print(f"Lens 데이터만 저장 완료: {lens_output_file}")
    
    return cleaned_inv_df, lens_df

if __name__ == "__main__":
    # 파일 경로 설정
    input_file = "data/inventory_final_anonymous.csv"
    output_file = "data/inventory_final_anonymous_cleaned.csv"
    
    # lens 데이터 정리 실행
    cleaned_df, lens_df = clean_lens_data(input_file, output_file)
    
    if cleaned_df is not None and lens_df is not None:
        # 결과 확인
        print("\n=== 정리 결과 확인 ===")
        print("정리된 Lens 데이터 샘플:")
        
        # 컬럼 찾기
        level1_col, level2_col, level3_col = find_category_columns(lens_df)
        
        result_columns = ['item_code', level1_col]
        if level2_col:
            result_columns.append(level2_col)
        if level3_col:
            result_columns.append(level3_col)
        
        print(lens_df[result_columns].head(10))
        
        if level2_col:
            print(f"\n{level2_col} 분포:")
            print(lens_df[level2_col].value_counts().head(10))
        
        if level3_col:
            print(f"\n{level3_col} 분포:")
            print(lens_df[level3_col].value_counts().head(10))
    else:
        print("데이터 정리를 완료할 수 없습니다.") 