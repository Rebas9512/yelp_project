#!/usr/bin/env bash
set -euo pipefail

echo "==> Scan for stray submodule artifacts..."
# 1) 删除根目录 .gitmodules（如有）
if [ -f .gitmodules ]; then
  echo "   - removing .gitmodules"
  rm -f .gitmodules
fi

# 2) 清理工作树里所有“嵌套 .git”
#    注意：仅删除子目录里的 .git（文件或目录），不会动仓库根的 .git
while IFS= read -r p; do
  if [ "$p" != "./.git" ]; then
    echo "   - removing $p"
    rm -rf "$p"
  fi
done < <(find . -mindepth 2 -maxdepth 3 \( -type d -name .git -o -type f -name .git \) -print)

echo "==> Re-add everything..."
git add -A

echo "==> Status:"
git status --porcelain=v1

echo "Done. Now run: git commit -m 'Initial commit'"
