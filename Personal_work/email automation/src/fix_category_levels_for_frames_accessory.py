import pandas as pd
import numpy as np
import re

def extract_category_from_item(item_str, desc_str):
    """
    Item과 Description에서 category_level_2와 category_level_3를 추출하는 함수
    """
    if pd.isna(item_str) and pd.isna(desc_str):
        return None, None
    
    # Item이나 Description 중 하나라도 있으면 사용
    text = str(item_str) if pd.notna(item_str) else str(desc_str)
    
    # NaN이나 빈 문자열 처리
    if text == 'nan' or text == '':
        return None, None
    
    # 하이픈(-)으로 분리
    parts = text.split('-')
    
    if len(parts) >= 2:
        # 첫 번째 부분이 category_level_2
        cat2 = parts[0].strip()
        # 나머지 부분들을 합쳐서 category_level_3
        cat3 = '-'.join(parts[1:]).strip()
        return cat2, cat3
    elif len(parts) == 1:
        # 하이픈이 없으면 전체를 category_level_2로
        cat2 = parts[0].strip()
        return cat2, None
    else:
        return None, None

def fix_category_levels_for_frames_accessory(input_file, output_file):
    """
    FRAMES와 ACCESSORY 데이터의 category_level_2, category_level_3를 수정하는 함수
    """
    
    print(f"데이터 로드 중: {input_file}")
    df = pd.read_csv(input_file)
    print(f"전체 데이터: {len(df)}행")
    
    # FRAMES와 ACCESSORY 데이터만 필터링
    frames_accessory_mask = df['Category_Level_1'].isin(['FRAMES', 'ACCESSORY'])
    frames_accessory_df = df[frames_accessory_mask].copy()
    
    print(f"FRAMES/ACCESSORY 데이터: {len(frames_accessory_df)}행")
    
    # 수정 전 샘플 확인
    print("\n=== 수정 전 샘플 ===")
    sample_before = frames_accessory_df[['item_code', 'Item', 'Description', 'Category_Level_1', 'Category_Level_2', 'Category_Level_3']].head(10)
    print(sample_before)
    
    # category_level_2, category_level_3 수정
    print("\n=== category_level_2, category_level_3 수정 중 ===")
    
    for idx, row in frames_accessory_df.iterrows():
        cat2, cat3 = extract_category_from_item(row['Item'], row['Description'])
        
        if cat2 is not None:
            frames_accessory_df.loc[idx, 'Category_Level_2'] = cat2
        if cat3 is not None:
            frames_accessory_df.loc[idx, 'Category_Level_3'] = cat3
    
    # 수정 후 샘플 확인
    print("\n=== 수정 후 샘플 ===")
    sample_after = frames_accessory_df[['item_code', 'Item', 'Description', 'Category_Level_1', 'Category_Level_2', 'Category_Level_3']].head(10)
    print(sample_after)
    
    # lens 데이터는 그대로 유지
    lens_mask = df['Category_Level_1'] == 'lens'
    lens_df = df[lens_mask].copy()
    
    # 기타 데이터 (FRAMES, ACCESSORY, lens가 아닌 데이터)
    other_mask = ~(df['Category_Level_1'].isin(['FRAMES', 'ACCESSORY', 'lens']))
    other_df = df[other_mask].copy()
    
    print(f"\n데이터 분류:")
    print(f"- FRAMES/ACCESSORY: {len(frames_accessory_df)}행")
    print(f"- LENS: {len(lens_df)}행")
    print(f"- 기타: {len(other_df)}행")
    
    # 모든 데이터 병합
    final_df = pd.concat([frames_accessory_df, lens_df, other_df], ignore_index=True)
    
    print(f"최종 데이터: {len(final_df)}행")
    
    # 결과 저장
    final_df.to_csv(output_file, index=False)
    print(f"\n수정된 데이터 저장 완료: {output_file}")
    
    # FRAMES/ACCESSORY 데이터만 별도 저장
    frames_accessory_output = output_file.replace('.csv', '_frames_accessory_only.csv')
    frames_accessory_df.to_csv(frames_accessory_output, index=False)
    print(f"FRAMES/ACCESSORY 데이터만 저장 완료: {frames_accessory_output}")
    
    return final_df, frames_accessory_df

def main():
    # 파일 경로 설정
    input_file = "../data/inventory_final_anonymous_updated.csv"
    output_file = "../data/inventory_final_anonymous_fixed.csv"
    
    # 수정 실행
    final_df, frames_accessory_df = fix_category_levels_for_frames_accessory(input_file, output_file)
    
    # 결과 요약
    print("\n=== 수정 결과 요약 ===")
    print("Category_Level_2 분포 (FRAMES/ACCESSORY):")
    print(frames_accessory_df['Category_Level_2'].value_counts().head(10))
    
    print("\nCategory_Level_3 분포 (FRAMES/ACCESSORY):")
    print(frames_accessory_df['Category_Level_3'].value_counts().head(10))

if __name__ == "__main__":
    main() 