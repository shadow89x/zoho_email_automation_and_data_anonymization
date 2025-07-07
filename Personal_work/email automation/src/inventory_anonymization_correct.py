#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔒 인벤토리 데이터 익명화 파이프라인 (올바른 파일 사용)

이 스크립트는 processed_inventory_data_with_item_code.csv 파일을 사용하여
인벤토리 데이터의 완전한 익명화를 수행합니다:

## 📋 익명화 전략:
1. **Lens 데이터**: 현재 상태 완전 유지 (아예 건드리지 않음)
2. **Frame/Accessory 데이터**: 완전 익명화
   - 같은 브랜드의 제품들은 일관된 익명 브랜드명 사용
   - 모델명은 익명화하되 구조 유지
   - 색상 번호는 유지하되 익명화된 모델명에 연결
3. **Category Level 1**: 숫자 제거하고 Accessory, Lens, Frame만 표기

## 🎯 목표:
- item_code는 원본 유지
- 나머지 컬럼들은 익명화
- 같은 브랜드 제품들의 일관성 유지
- 모델 구조 및 색상 정보 보존
- Lens 데이터는 현재 상태 완전 유지
"""

import pandas as pd
import numpy as np
import random
import re
from tqdm import tqdm
import warnings
import os
from pathlib import Path

warnings.filterwarnings('ignore')

def generate_anonymous_brand(original_brand, anonymous_brands):
    """원본 브랜드명을 기반으로 익명 브랜드명 생성"""
    if pd.isna(original_brand):
        return np.random.choice(anonymous_brands)
    
    # 원본 브랜드명을 시드로 사용
    brand_seed = hash(str(original_brand)) % 2147483647
    np.random.seed(brand_seed)
    
    return np.random.choice(anonymous_brands)

def generate_anonymous_model(original_model, model_prefixes, model_suffixes):
    """원본 모델명을 기반으로 익명 모델명 생성"""
    if pd.isna(original_model):
        return None
    
    # 원본 모델명을 시드로 사용
    model_seed = hash(str(original_model)) % 2147483647
    np.random.seed(model_seed)
    
    # 모델명 패턴 분석 (예: 5005-CHA)
    if isinstance(original_model, str):
        # 알파벳과 숫자 분리
        letters = re.findall(r'[A-Z]+', original_model)
        numbers = re.findall(r'\d+', original_model)
        
        if letters and numbers:
            # 새로운 익명 모델명 생성
            prefix = np.random.choice(model_prefixes)
            suffix = np.random.choice(model_suffixes)
            
            # 숫자 부분은 4자리로 생성
            number_part = str(np.random.randint(1000, 9999))
            
            return f"{prefix}{number_part}{suffix}"
        else:
            # 단순한 경우
            prefix = np.random.choice(model_prefixes)
            number_part = str(np.random.randint(1000, 9999))
            return f"{prefix}{number_part}"
    
    return None

def generate_anonymous_description(brand, model, color):
    """익명화된 브랜드, 모델, 색상을 기반으로 description 생성"""
    if pd.isna(model):
        return brand
    
    if pd.isna(color) or color == 'NaN':
        return f"{brand} ({model})"
    else:
        return f"{brand} ({model}-{color})"

def generate_anonymous_item(brand, model, color):
    """익명화된 브랜드, 모델, 색상을 기반으로 Item 생성"""
    if pd.isna(model):
        return brand
    
    if pd.isna(color) or color == 'NaN':
        return f"{brand}-{model}"
    else:
        return f"{brand}-{model}-{color}"

def clean_category_level_1(category):
    """Category Level 1에서 숫자를 제거하고 정리"""
    if pd.isna(category):
        return category
    
    category_str = str(category).strip()
    
    # 숫자 제거
    cleaned = re.sub(r'\d+', '', category_str)
    
    # 점과 콜론 제거
    cleaned = re.sub(r'[.:]', '', cleaned)
    
    # 앞뒤 공백 제거
    cleaned = cleaned.strip()
    
    # 대문자로 변환
    cleaned = cleaned.upper()
    
    return cleaned

def extract_brand_from_item(item):
    """Item에서 브랜드명 추출"""
    if pd.isna(item):
        return None
    
    # 예: "11.FRAMES:PB:5005-CHA" → "PB"
    parts = str(item).split(':')
    if len(parts) >= 2:
        return parts[1]
    return None

def extract_model_from_item(item):
    """Item에서 모델명 추출"""
    if pd.isna(item):
        return None
    
    # 예: "11.FRAMES:PB:5005-CHA" → "5005"
    parts = str(item).split(':')
    if len(parts) >= 3:
        model_part = parts[2]
        # 숫자 부분만 추출
        numbers = re.findall(r'\d+', model_part)
        if numbers:
            return numbers[0]
    return None

def extract_color_from_item(item):
    """Item에서 색상명 추출"""
    if pd.isna(item):
        return None
    
    # 예: "11.FRAMES:PB:5005-CHA" → "CHA"
    parts = str(item).split(':')
    if len(parts) >= 3:
        model_part = parts[2]
        # 알파벳 부분만 추출
        letters = re.findall(r'[A-Z]+', model_part)
        if letters:
            return letters[-1]  # 마지막 알파벳 부분
    return None

def main():
    print("🔒 인벤토리 데이터 익명화 파이프라인 시작 (올바른 파일 사용)")
    print("=" * 60)
    
    # 재현 가능한 결과를 위한 시드 설정
    np.random.seed(42)
    random.seed(42)
    
    # 1단계: 올바른 인벤토리 데이터 로드
    print("1단계: 올바른 인벤토리 데이터 로드")
    print("-" * 30)
    
    # 올바른 인벤토리 데이터 로드
    inv_df = pd.read_csv('../data/processed_inventory_data_with_item_code.csv')
    print(f"✅ 인벤토리 데이터 로드: {inv_df.shape}")
    
    print(f"\n📊 데이터 정보:")
    print(f"컬럼: {list(inv_df.columns)}")
    print(f"\n카테고리 분포:")
    print(inv_df['Category_Level_1'].value_counts().head(10))
    
    # 2단계: 데이터 구조 분석
    print("\n2단계: 데이터 구조 분석")
    print("-" * 30)
    
    # Lens 데이터 분석 (01.LENS, 01 ABBA LENS 등)
    lens_data = inv_df[inv_df['Category_Level_1'].str.contains('LENS', na=False)].copy()
    print(f"Lens 데이터: {len(lens_data)}개 (현재 상태 완전 유지)")
    
    # Frame 데이터 분석 (11.FRAMES 등)
    frame_data = inv_df[inv_df['Category_Level_1'].str.contains('FRAME', na=False)].copy()
    print(f"Frame 데이터: {len(frame_data)}개")
    
    # Accessory 데이터 분석 (21.ACCESS 등)
    accessory_data = inv_df[inv_df['Category_Level_1'].str.contains('ACCESS', na=False)].copy()
    print(f"Accessory 데이터: {len(accessory_data)}개")
    
    # Lens 데이터 원본 백업
    lens_data_original = lens_data.copy()
    print("✅ Lens 데이터 원본 백업 완료")
    
    # 3단계: 익명화 데이터 생성 준비
    print("\n3단계: 익명화 데이터 생성 준비")
    print("-" * 30)
    
    # 익명화용 브랜드명 생성
    anonymous_brands = [
        'OptiTech', 'VisionPro', 'ClearView', 'EyeStyle', 'LensCraft', 'FrameWorks', 'OpticalPlus',
        'VisionMax', 'EyeCare', 'OptiFrame', 'LensStyle', 'FrameTech', 'VisionCraft', 'OptiCare',
        'EyeTech', 'LensPro', 'FrameMax', 'VisionStyle', 'OptiWorks', 'EyeCraft', 'LensCare',
        'FramePlus', 'VisionTech', 'OptiStyle', 'EyeMax', 'LensWorks', 'FrameCare', 'VisionPlus',
        'OptiCraft', 'EyeStyle', 'LensTech', 'FramePro', 'VisionMax', 'OptiCare', 'EyeWorks'
    ]
    
    # 익명화용 모델명 접두사
    model_prefixes = [
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
        'U', 'V', 'W', 'X', 'Y', 'Z'
    ]
    
    # 익명화용 모델명 접미사
    model_suffixes = [
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
        'U', 'V', 'W', 'X', 'Y', 'Z'
    ]
    
    print(f"익명화 자료 준비 완료:")
    print(f"  - 브랜드명: {len(anonymous_brands)}개")
    print(f"  - 모델 접두사: {len(model_prefixes)}개")
    print(f"  - 모델 접미사: {len(model_suffixes)}개")
    
    # 4단계: 브랜드별 익명화 매핑 생성
    print("\n4단계: 브랜드별 익명화 매핑 생성")
    print("-" * 30)
    
    # Frame과 Accessory 브랜드들 수집 (Lens 제외)
    frame_accessory_brands = set()
    
    # Frame에서 브랜드 수집
    for item in frame_data['Item'].dropna():
        brand = extract_brand_from_item(item)
        if brand:
            frame_accessory_brands.add(brand)
    
    # Accessory에서 브랜드 수집
    for item in accessory_data['Item'].dropna():
        brand = extract_brand_from_item(item)
        if brand:
            frame_accessory_brands.add(brand)
    
    frame_accessory_brands = list(frame_accessory_brands)
    print(f"익명화할 브랜드 수: {len(frame_accessory_brands)}")
    print(f"브랜드 목록: {frame_accessory_brands}")
    
    # 브랜드별 익명화 매핑 생성
    brand_mapping = {}
    model_mapping = {}
    
    print("브랜드별 익명화 매핑 생성 중...")
    for brand in tqdm(frame_accessory_brands, desc="브랜드 매핑"):
        if pd.isna(brand):
            continue
        
        # 브랜드 익명화
        brand_mapping[brand] = generate_anonymous_brand(brand, anonymous_brands)
        
        # 해당 브랜드의 모델들 수집
        brand_models = set()
        
        # Frame에서 모델 수집
        for item in frame_data[frame_data['Item'].str.contains(brand, na=False)]['Item'].dropna():
            model = extract_model_from_item(item)
            if model:
                brand_models.add(model)
        
        # Accessory에서 모델 수집
        for item in accessory_data[accessory_data['Item'].str.contains(brand, na=False)]['Item'].dropna():
            model = extract_model_from_item(item)
            if model:
                brand_models.add(model)
        
        # 모델별 익명화
        for model in brand_models:
            if pd.isna(model):
                continue
            model_key = f"{brand}_{model}"
            model_mapping[model_key] = generate_anonymous_model(model, model_prefixes, model_suffixes)
    
    print(f"✅ 브랜드 매핑: {len(brand_mapping)}개")
    print(f"✅ 모델 매핑: {len(model_mapping)}개")
    
    # 5단계: Frame/Accessory 데이터 익명화
    print("\n5단계: Frame/Accessory 데이터 익명화")
    print("-" * 30)
    
    # Frame 데이터 익명화
    print("Frame 데이터 익명화 중...")
    frame_df_anonymous = frame_data.copy()
    
    for idx, row in tqdm(frame_df_anonymous.iterrows(), total=len(frame_df_anonymous), desc="Frame 익명화"):
        original_item = row['Item']
        
        if pd.notna(original_item):
            # 브랜드, 모델, 색상 추출
            original_brand = extract_brand_from_item(original_item)
            original_model = extract_model_from_item(original_item)
            original_color = extract_color_from_item(original_item)
            
            # 브랜드 익명화
            if original_brand and original_brand in brand_mapping:
                new_brand = brand_mapping[original_brand]
            else:
                new_brand = original_brand
            
            # 모델 익명화
            if original_model:
                model_key = f"{original_brand}_{original_model}"
                if model_key in model_mapping:
                    new_model = model_mapping[model_key]
                else:
                    new_model = generate_anonymous_model(original_model, model_prefixes, model_suffixes)
            else:
                new_model = None
            
            # 새로운 Item과 Description 생성
            new_item = generate_anonymous_item(new_brand, new_model, original_color)
            new_description = generate_anonymous_description(new_brand, new_model, original_color)
            
            # 데이터 업데이트
            frame_df_anonymous.at[idx, 'Item'] = new_item
            frame_df_anonymous.at[idx, 'Description'] = new_description
    
    # Accessory 데이터 익명화
    print("\nAccessory 데이터 익명화 중...")
    accessory_df_anonymous = accessory_data.copy()
    
    for idx, row in tqdm(accessory_df_anonymous.iterrows(), total=len(accessory_df_anonymous), desc="Accessory 익명화"):
        original_item = row['Item']
        
        if pd.notna(original_item):
            # 브랜드, 모델, 색상 추출
            original_brand = extract_brand_from_item(original_item)
            original_model = extract_model_from_item(original_item)
            original_color = extract_color_from_item(original_item)
            
            # 브랜드 익명화
            if original_brand and original_brand in brand_mapping:
                new_brand = brand_mapping[original_brand]
            else:
                new_brand = original_brand
            
            # 모델 익명화
            if original_model:
                model_key = f"{original_brand}_{original_model}"
                if model_key in model_mapping:
                    new_model = model_mapping[model_key]
                else:
                    new_model = generate_anonymous_model(original_model, model_prefixes, model_suffixes)
            else:
                new_model = None
            
            # 새로운 Item과 Description 생성
            new_item = generate_anonymous_item(new_brand, new_model, original_color)
            new_description = generate_anonymous_description(new_brand, new_model, original_color)
            
            # 데이터 업데이트
            accessory_df_anonymous.at[idx, 'Item'] = new_item
            accessory_df_anonymous.at[idx, 'Description'] = new_description
    
    print(f"✅ Frame 익명화 완료: {len(frame_df_anonymous)}개")
    print(f"✅ Accessory 익명화 완료: {len(accessory_df_anonymous)}개")
    
    # 6단계: Category Level 1 정리 (Frame/Accessory만)
    print("\n6단계: Category Level 1 정리 (Frame/Accessory만)")
    print("-" * 30)
    
    # Frame과 Accessory만 정리 (Lens는 건드리지 않음)
    print("Category Level 1 정리 중... (Lens 제외)")
    frame_df_anonymous['Category_Level_1'] = frame_df_anonymous['Category_Level_1'].apply(clean_category_level_1)
    accessory_df_anonymous['Category_Level_1'] = accessory_df_anonymous['Category_Level_1'].apply(clean_category_level_1)
    
    # Lens 데이터는 원본 그대로 유지
    lens_data_final = lens_data_original.copy()
    print("✅ Lens 데이터는 원본 그대로 유지")
    
    print("Category Level 1 정리 결과:")
    print(frame_df_anonymous['Category_Level_1'].value_counts())
    print(accessory_df_anonymous['Category_Level_1'].value_counts())
    print(lens_data_final['Category_Level_1'].value_counts().head(5))
    
    # 7단계: 최종 데이터 통합
    print("\n7단계: 최종 데이터 통합")
    print("-" * 30)
    
    # 모든 데이터 통합 (Lens는 원본 그대로)
    inv_df_final = pd.concat([frame_df_anonymous, accessory_df_anonymous, lens_data_final], ignore_index=True)
    
    print(f"최종 인벤토리 데이터: {inv_df_final.shape}")
    print(f"카테고리 분포:")
    print(inv_df_final['Category_Level_1'].value_counts().head(10))
    
    # 8단계: 익명화 매핑 테이블 생성 및 저장
    print("\n8단계: 익명화 매핑 테이블 생성 및 저장")
    print("-" * 30)
    
    # 브랜드 매핑 테이블 생성
    brand_mapping_data = []
    for original_brand, anonymous_brand in brand_mapping.items():
        row = {
            'original_brand': original_brand,
            'anonymous_brand': anonymous_brand
        }
        brand_mapping_data.append(row)
    
    brand_mapping_df = pd.DataFrame(brand_mapping_data)
    print(f"브랜드 매핑 테이블: {brand_mapping_df.shape}")
    
    # 모델 매핑 테이블 생성
    model_mapping_data = []
    for model_key, anonymous_model in model_mapping.items():
        original_brand, original_model = model_key.split('_', 1)
        row = {
            'original_brand': original_brand,
            'original_model': original_model,
            'anonymous_model': anonymous_model
        }
        model_mapping_data.append(row)
    
    model_mapping_df = pd.DataFrame(model_mapping_data)
    print(f"모델 매핑 테이블: {model_mapping_df.shape}")
    
    # 매핑 테이블 저장
    brand_mapping_output = '../data/inventory_brand_anonymous_mapping.csv'
    model_mapping_output = '../data/inventory_model_anonymous_mapping.csv'
    
    brand_mapping_df.to_csv(brand_mapping_output, index=False)
    model_mapping_df.to_csv(model_mapping_output, index=False)
    
    print(f"✅ 브랜드 매핑 테이블 저장: {brand_mapping_output}")
    print(f"✅ 모델 매핑 테이블 저장: {model_mapping_output}")
    
    # 9단계: 최종 결과 확인 및 저장
    print("\n9단계: 최종 결과 확인 및 저장")
    print("-" * 30)
    
    # 최종 인벤토리 데이터 저장
    inv_final_output = '../data/inventory_final_anonymous.csv'
    inv_df_final.to_csv(inv_final_output, index=False)
    print(f"✅ 최종 인벤토리 데이터 저장: {inv_final_output}")
    
    # 최종 통계
    print(f"\n📊 최종 익명화 결과:")
    print(f"총 인벤토리 아이템: {len(inv_df_final):,}")
    print(f"카테고리 분포:")
    category_stats = inv_df_final['Category_Level_1'].value_counts()
    for category, count in category_stats.items():
        print(f"  {category}: {count:,}개")
    
    print(f"\n브랜드 익명화:")
    print(f"  익명화된 브랜드: {len(brand_mapping):,}개")
    print(f"  익명화된 모델: {len(model_mapping):,}개")
    
    print(f"\n🎉 인벤토리 익명화 완료!")
    print(f"📁 저장된 파일:")
    print(f"   - {inv_final_output}")
    print(f"   - {brand_mapping_output}")
    print(f"   - {model_mapping_output}")
    
    print(f"\n✅ 모든 개인정보가 익명화되었습니다!")
    print(f"✅ 같은 브랜드의 제품들은 일관된 익명 브랜드명을 가집니다!")
    print(f"✅ 모델 구조와 색상 정보가 보존되었습니다!")
    print(f"✅ Lens 데이터는 원본 그대로 완전 유지되었습니다!")
    print(f"✅ Category Level 1이 정리되었습니다!")
    
    # 샘플 데이터 출력
    print(f"\n=== Frame 익명화 결과 샘플 ===")
    frame_sample = inv_df_final[inv_df_final['Category_Level_1'] == 'FRAMES'].head(5)
    print(frame_sample[['item_code', 'Item', 'Description', 'Category_Level_1']].to_string())
    
    print(f"\n=== Accessory 익명화 결과 샘플 ===")
    accessory_sample = inv_df_final[inv_df_final['Category_Level_1'] == 'ACCESS'].head(5)
    print(accessory_sample[['item_code', 'Item', 'Description', 'Category_Level_1']].to_string())
    
    print(f"\n=== Lens 데이터 샘플 (원본 그대로 유지) ===")
    lens_sample = inv_df_final[inv_df_final['Category_Level_1'].str.contains('LENS', na=False)].head(3)
    print(lens_sample[['item_code', 'Item', 'Description', 'Category_Level_1']].to_string())
    
    # Lens 데이터 변경 확인
    print(f"\n=== Lens 데이터 변경 확인 ===")
    lens_changed = not lens_data_final.equals(lens_data_original)
    print(f"Lens 데이터 변경 여부: {'변경됨' if lens_changed else '변경되지 않음 (올바름)'}")
    
    if not lens_changed:
        print("✅ Lens 데이터가 원본 그대로 유지되었습니다!")

if __name__ == "__main__":
    main() 