import subprocess
import os

def commit_and_push(file_path, commit_message):
    """
    Thực hiện git add, commit, và push cho một file cụ thể.
    Chỉ hoạt động trong môi trường GitHub Actions.
    """
    # Kiểm tra xem có đang chạy trên GitHub Actions không
    if 'CI' not in os.environ:
        print("Không phải môi trường GitHub Actions, bỏ qua commit và push.")
        return

    print(f"Bắt đầu commit và push file '{file_path}'...")
    try:
        # Cấu hình git user (cần thiết cho CI)
        subprocess.run(['git', 'config', '--global', 'user.name', 'github-actions[bot]'], check=True)
        subprocess.run(['git', 'config', '--global', 'user.email', 'github-actions[bot]@users.noreply.github.com'], check=True)
        
        # Add, commit, push
        subprocess.run(['git', 'add', file_path], check=True)
        # Kiểm tra xem có thay đổi gì để commit không
        status_result = subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True)
        if file_path in status_result.stdout:
            subprocess.run(['git', 'commit', '-m', commit_message], check=True)
            subprocess.run(['git', 'push'], check=True)
            print("Commit và push thành công.")
        else:
            print("Không có thay đổi để commit.")

    except subprocess.CalledProcessError as e:
        print(f"Lỗi khi thực thi lệnh git: {e}")
    except Exception as e:
        print(f"Lỗi không xác định khi commit/push: {e}")