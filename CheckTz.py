import locale
import os
import sys
import tempfile
import time
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None


def check_environment():
    now = datetime.now()
    utc_now = datetime.utcnow()

    result = {
        "python_version": sys.version,
        "timezone": {
            "tzname": time.tzname,
            "local_datetime": now.isoformat(),
            "utc_datetime": utc_now.isoformat(),
            "timezone_env": os.getenv("TZ"),
        },
        "charset": {
            "default_encoding": sys.getdefaultencoding(),
            "filesystem_encoding": sys.getfilesystemencoding(),
            "stdout_encoding": sys.stdout.encoding,
            "preferred_encoding": locale.getpreferredencoding(False),
            "locale": locale.setlocale(locale.LC_ALL, None),
            "lang_env": os.getenv("LANG"),
            "lc_all_env": os.getenv("LC_ALL"),
        },
    }

    # Asia/Seoul 사용 가능 여부
    if ZoneInfo:
        try:
            seoul_now = datetime.now(ZoneInfo("Asia/Seoul"))

            result["timezone"]["asia_seoul_available"] = True
            result["timezone"]["asia_seoul_datetime"] = seoul_now.isoformat()
            result["timezone"]["utc_offset"] = str(seoul_now.utcoffset())

        except Exception as e:
            result["timezone"]["asia_seoul_available"] = False
            result["timezone"]["error"] = str(e)

    # 한글 encode/decode 테스트
    korean_text = "한글 테스트입니다. AI 모델 서빙 정상"

    try:
        encoded = korean_text.encode("utf-8")
        decoded = encoded.decode("utf-8")

        result["charset"]["korean_utf8_test"] = decoded == korean_text
        result["charset"]["korean_text"] = decoded

    except Exception as e:
        result["charset"]["korean_utf8_test"] = False
        result["charset"]["korean_error"] = str(e)

    # 한글 파일명 + 한글 내용 테스트
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(
                tmpdir,
                "한글_테스트.txt",
            )

            with open(
                file_path,
                "w",
                encoding="utf-8",
            ) as f:
                f.write(korean_text)

            with open(
                file_path,
                "r",
                encoding="utf-8",
            ) as f:
                read_text = f.read()

            result["charset"]["korean_filename_test"] = os.path.exists(file_path)
            result["charset"]["korean_file_content_test"] = read_text == korean_text

    except Exception as e:
        result["charset"]["korean_file_test"] = False
        result["charset"]["korean_file_error"] = str(e)

    return result
