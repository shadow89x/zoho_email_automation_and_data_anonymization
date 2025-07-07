import pandas as pd
import re

def extract_business_info(account_no):
    """
    Extract base business number and account type from account number.
    
    What: Parses account numbers to extract the numeric base, optional suffix, and maps to account type.
    Why: Business/account IDs are often encoded in a single string; splitting them enables grouping and analysis.
    How: Uses regex to split number and suffix, then maps suffix to type.
    Alternative: Could use more advanced parsing for non-standard formats, but regex covers most cases here.
    """
    if pd.isna(account_no):
        return None, None, None
    
    account_str = str(account_no)
    
    # 알파벳 접미사 패턴 찾기
    match = re.match(r'(\d+)([A-Za-z]*)$', account_str)
    
    if match:
        base_number = match.group(1)  # 기본 번호 (예: 1341)
        suffix = match.group(2).upper() if match.group(2) else ''  # 접미사 (예: A, F, K, S, E)
        
        # 계정 타입 분류
        account_type_map = {
            'A': 'Accessory',
            'F': 'Frame', 
            'K': 'Surface',
            'S': 'Brand Lens',
            'E': 'Edging',
            '': 'Lens'  # 접미사가 없으면 렌즈
        }
        
        account_type = account_type_map.get(suffix, 'Other')
        
        return base_number, suffix, account_type
    
    return account_str, '', 'Unknown'

def process_account_data(df):
    """
    Process account numbers in DataFrame to create business IDs.
    
    What: Adds business info columns and assigns unique business IDs based on account numbers.
    Why: Enables grouping, deduplication, and downstream analysis by business.
    How: Extracts info, creates mapping, adds columns. Only shape/columns are printed for privacy.
    Alternative: Could use hash-based IDs, but sequential IDs are more interpretable for humans.
    """
    # df에 비즈니스 정보 추가
    df[['base_account', 'suffix', 'account_type']] = df['Account No.'].apply(
        lambda x: pd.Series(extract_business_info(x))
    )

    # 고유한 비즈니스 ID 생성
    unique_base_accounts = df['base_account'].dropna().unique()
    business_id_map = {base: f"BUS_{i+1:04d}" for i, base in enumerate(sorted(unique_base_accounts))}

    # 비즈니스 ID 추가
    df['business_id'] = df['base_account'].map(business_id_map)

    # 데이터 분석을 위한 추가 컬럼들
    df['is_main_account'] = df['suffix'] == ''  # 메인 계정 여부 (렌즈)
    df['has_multiple_accounts'] = df.groupby('business_id')['business_id'].transform('count') > 1
    
    print(f"df: {df.shape}, columns: {list(df.columns)}")
    return df

def analyze_business_data(df):
    """
    Analyze processed business/account data and print summary statistics.
    
    What: Provides descriptive stats and sample groupings for business/account structure.
    Why: Helps understand data distribution and quality for downstream tasks.
    How: Prints summary stats, sample groupings, and only shape/columns for privacy.
    Alternative: Could visualize with plots, but tabular stats are sufficient for most checks.
    """
    print("=== 계정 번호 처리 결과 ===")
    print(f"총 고유 비즈니스 수: {df['business_id'].nunique()}")
    print(f"총 계정 수: {len(df)}")

    print("\n=== 계정 타입별 분포 ===")
    account_type_counts = df['account_type'].value_counts()
    print(account_type_counts)

    print("\n=== 샘플 데이터 (처리 후) ===")
    sample_columns = ['Customer', 'Account No.', 'base_account', 'suffix', 'account_type', 'business_id']
    print(df[sample_columns].head(10))

    print("\n=== 같은 비즈니스의 여러 계정 예시 ===")
    # 같은 비즈니스 ID를 가진 계정들 그룹핑
    business_groups = df.groupby('business_id').agg({
        'Customer': 'first',
        'Account No.': list,
        'account_type': list
    }).head(5)

    for idx, row in business_groups.iterrows():
        print(f"\n비즈니스 ID: {idx}")
        print(f"고객명: {row['Customer']}")
        print(f"계정들: {row['Account No.']}")
        print(f"계정 타입들: {row['account_type']}")

    print("\n=== 데이터 분석용 컬럼 추가 완료 ===")
    print("새로 추가된 컬럼들:")
    print("- base_account: 기본 계정 번호")
    print("- suffix: 계정 접미사 (A, F, K, S, E 등)")
    print("- account_type: 계정 타입 (Lens, Frame, Accessory 등)")
    print("- business_id: 고유 비즈니스 ID")
    print("- is_main_account: 메인 계정 여부")
    print("- has_multiple_accounts: 다중 계정 보유 여부")

    # 최종 데이터 확인
    print(f"\n=== 최종 DataFrame 정보 ===")
    print(f"Shape: {df.shape}")
    print(f"컬럼 수: {len(df.columns)}")
    print(f"고유 비즈니스 수: {df['business_id'].nunique()}")
    print(f"다중 계정 보유 비즈니스 수: {df[df['has_multiple_accounts']]['business_id'].nunique()}")

    # 데이터 분석 예시
    print("\n=== 데이터 분석 예시 ===")

    # 1. 비즈니스별 계정 수 분포
    account_count_by_business = df.groupby('business_id').size().value_counts().sort_index()
    print("1. 비즈니스별 계정 수 분포:")
    print(account_count_by_business)

    # 2. 계정 타입별 비즈니스 수
    business_by_type = df.groupby('account_type')['business_id'].nunique().sort_values(ascending=False)
    print("\n2. 계정 타입별 비즈니스 수:")
    print(business_by_type)

    # 3. 다중 계정 보유 비즈니스의 계정 구성
    multi_account_businesses = df[df['has_multiple_accounts']].groupby('business_id').agg({
        'Customer': 'first',
        'account_type': list,
        'Account No.': list
    }).head(10)

    print("\n3. 다중 계정 보유 비즈니스 예시 (상위 10개):")
    for idx, row in multi_account_businesses.iterrows():
        print(f"\n{row['Customer']} (ID: {idx})")
        print(f"  계정들: {row['Account No.']}")
        print(f"  타입들: {row['account_type']}")

    # 4. 메인 계정(렌즈)이 있는 비즈니스 수
    main_account_businesses = df[df['is_main_account']]['business_id'].nunique()
    print(f"\n4. 메인 계정(렌즈) 보유 비즈니스 수: {main_account_businesses}")

    # 5. 계정 타입별 평균 계정 수
    type_stats = df.groupby('account_type').agg({
        'business_id': ['count', 'nunique']
    }).round(2)
    type_stats.columns = ['total_accounts', 'unique_businesses']
    type_stats['avg_per_business'] = (type_stats['total_accounts'] / type_stats['unique_businesses']).round(2)

    print("\n5. 계정 타입별 통계:")
    print(type_stats)

    print(f"df: {df.shape}, columns: {list(df.columns)}")

if __name__ == "__main__":
    # CSV 파일 불러오기
    df = pd.read_csv('cc (1).csv')
    
    # 계정 데이터 처리
    df_processed = process_account_data(df)
    
    # 분석 실행
    analyze_business_data(df_processed)
    
    # 처리된 데이터 저장 (선택사항)
    df_processed.to_csv('processed_accounts.csv', index=False)
    print("\n=== 처리된 데이터가 'processed_accounts.csv'로 저장되었습니다 ===") 