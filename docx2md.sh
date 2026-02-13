#!/bin/bash

echo "======================================"
echo " DOCX → Markdown 批量转换开始"
echo "======================================"

count=0
success=0
fail=0

for f in doc/*.docx; do
  name=$(basename "$f" .docx)

  count=$((count+1))

  echo "--------------------------------------"
  echo "[$count] 正在转换: $f"
  echo "输出文件: md/$name.md"
  echo "图片目录: media/$name"

  if pandoc "$f" \
    -t gfm \
    -o "md/$name.md" \
    --extract-media="media/$name" \
    --mathjax; then

    echo "✅ 转换成功: $name"
    success=$((success+1))
  else
    echo "❌ 转换失败: $name"
    fail=$((fail+1))
  fi
done

echo "======================================"
echo "转换完成"
echo "总文件数: $count"
echo "成功: $success"
echo "失败: $fail"
echo "======================================"

