import os
import sys
import argparse
import zipfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import fitz  # PyMuPDF


def convert_page(args):
    """단일 페이지를 이미지로 변환"""
    pdf_path, page_num, dpi, img_format, quality = args

    try:
        doc = fitz.open(pdf_path)
        page = doc[page_num]

        # DPI를 zoom으로 변환 (72 DPI가 기본, 150 DPI면 150/72 = 2.08배)
        zoom = dpi / 72
        mat = fitz.Matrix(zoom, zoom)

        # 페이지를 이미지로 렌더링
        pix = page.get_pixmap(matrix=mat, alpha=False)

        # 임시 메모리에 이미지 데이터 저장
        if img_format.lower() == 'png':
            img_data = pix.tobytes("png")
            ext = 'png'
        else:
            img_data = pix.tobytes("jpeg", jpg_quality=quality)
            ext = 'jpg'

        doc.close()

        return page_num, img_data, ext, None
    except Exception as e:
        return page_num, None, None, str(e)


def convert_pdf_to_images(pdf_path, output_dir=None, img_format='jpeg', dpi=150,
                          quality=85, create_zip=True, num_workers=None):
    """
    PDF를 이미지로 변환

    Args:
        pdf_path: PDF 파일 경로
        output_dir: 출력 디렉토리 (None이면 PDF와 같은 위치)
        img_format: 이미지 형식 ('jpeg' 또는 'png')
        dpi: 이미지 해상도 (기본 150, 높을수록 고품질)
        quality: JPEG 품질 (1-100, PNG는 무시됨)
        create_zip: ZIP 파일로 압축할지 여부
        num_workers: 병렬 처리 워커 수 (None이면 CPU 코어 수)
    """
    pdf_path = Path(pdf_path)

    if not pdf_path.exists():
        print(f"오류: 파일을 찾을 수 없습니다 - {pdf_path}")
        return False

    # 출력 디렉토리 설정
    if output_dir is None:
        output_dir = pdf_path.parent / f"{pdf_path.stem}_images"
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(exist_ok=True)

    # PDF 정보 로드
    try:
        doc = fitz.open(str(pdf_path))
        num_pages = len(doc)
        doc.close()

        print(f"\n📄 PDF 파일: {pdf_path.name}")
        print(f"📊 총 페이지 수: {num_pages}")
        print(f"🎨 이미지 형식: {img_format.upper()}")
        print(f"🔍 해상도: {dpi} DPI")
        if img_format.lower() == 'jpeg':
            print(f"⭐ 품질: {quality}")
        print(f"⚙️  병렬 처리: {num_workers or os.cpu_count()} 워커\n")

    except Exception as e:
        print(f"오류: PDF를 열 수 없습니다 - {e}")
        return False

    # 워커 수 설정
    if num_workers is None:
        num_workers = os.cpu_count()

    # 페이지 변환 작업 준비
    tasks = [(str(pdf_path), i, dpi, img_format, quality) for i in range(num_pages)]

    # 병렬 처리로 페이지 변환
    results = []
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(convert_page, task): task for task in tasks}

        with tqdm(total=num_pages, desc="🔄 변환 중", unit="페이지") as pbar:
            for future in as_completed(futures):
                page_num, img_data, ext, error = future.result()

                if error:
                    print(f"\n⚠️  페이지 {page_num + 1} 변환 실패: {error}")
                else:
                    results.append((page_num, img_data, ext))

                pbar.update(1)

    # 결과를 페이지 순서대로 정렬
    results.sort(key=lambda x: x[0])

    # 이미지 파일 저장
    saved_files = []
    ext = results[0][2] if results else 'jpg'

    for page_num, img_data, _ in results:
        filename = f"page_{page_num + 1:03d}.{ext}"
        filepath = output_dir / filename

        with open(filepath, 'wb') as f:
            f.write(img_data)

        saved_files.append(filepath)

    print(f"\n✅ {len(saved_files)}개 페이지 변환 완료!")
    print(f"📁 저장 위치: {output_dir}")

    # ZIP 파일 생성
    if create_zip and saved_files:
        zip_path = pdf_path.parent / f"{pdf_path.stem}_images.zip"

        print(f"\n📦 ZIP 파일 생성 중...")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in tqdm(saved_files, desc="압축 중", unit="파일"):
                zipf.write(file, file.name)

        print(f"✅ ZIP 파일 생성 완료: {zip_path}")

        # 원본 이미지 파일 삭제 여부 확인
        print(f"\n💾 원본 이미지 폴더 유지: {output_dir}")

    return True


def main():
    parser = argparse.ArgumentParser(
        description='PDF를 이미지로 변환하는 도구',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
사용 예시:
  %(prog)s input.pdf                           # 기본 설정으로 변환
  %(prog)s input.pdf -f png                    # PNG 형식으로 변환
  %(prog)s input.pdf -d 300 -q 95              # 고품질로 변환
  %(prog)s input.pdf -o ./output               # 출력 디렉토리 지정
  %(prog)s input.pdf --no-zip                  # ZIP 파일 생성 안 함
  %(prog)s input.pdf -w 8                      # 8개 워커로 병렬 처리
        '''
    )

    parser.add_argument('pdf_file', help='변환할 PDF 파일')
    parser.add_argument('-o', '--output', help='출력 디렉토리 (기본: PDF파일명_images)')
    parser.add_argument('-f', '--format', choices=['jpeg', 'png'], default='jpeg',
                        help='이미지 형식 (기본: jpeg)')
    parser.add_argument('-d', '--dpi', type=int, default=150,
                        help='이미지 해상도 DPI (기본: 150, 권장: 150-300)')
    parser.add_argument('-q', '--quality', type=int, default=85,
                        help='JPEG 품질 (1-100, 기본: 85)')
    parser.add_argument('--no-zip', action='store_true',
                        help='ZIP 파일 생성 안 함')
    parser.add_argument('-w', '--workers', type=int, default=None,
                        help=f'병렬 처리 워커 수 (기본: CPU 코어 수 {os.cpu_count()}개)')

    args = parser.parse_args()

    # 품질 값 검증
    if args.quality < 1 or args.quality > 100:
        print("오류: 품질 값은 1-100 사이여야 합니다.")
        sys.exit(1)

    # DPI 값 검증
    if args.dpi < 72:
        print("경고: DPI가 너무 낮습니다. 최소 72 이상 권장합니다.")
    elif args.dpi > 600:
        print("경고: DPI가 너무 높으면 파일 크기가 매우 커집니다.")

    # 변환 실행
    success = convert_pdf_to_images(
        pdf_path=args.pdf_file,
        output_dir=args.output,
        img_format=args.format,
        dpi=args.dpi,
        quality=args.quality,
        create_zip=not args.no_zip,
        num_workers=args.workers
    )

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
