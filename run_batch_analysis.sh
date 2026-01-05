#!/bin/bash

# 确保脚本出错时停止
set -e

# 编译项目
echo "Building project..."
make build

# 定义 EC 策略 (Data Parity)
# 格式: "Data Parity"
EC_CONFIGS=(
    "2 1"
    "4 2"
    "6 3"
    "10 4"
    "20 4"
)

# 定义块大小 (Bytes)
# 4KB, 32KB, 1MB, 4MB, 16MB, 64MB
# 使用关联数组存储 标签->字节数 映射 (Bash 4.0+)
declare -A BLOCK_SIZES
BLOCK_SIZES=(
    ["4KB"]=4096
    ["32KB"]=32768
    ["1MB"]=1048576
    ["4MB"]=4194304
    ["16MB"]=16777216
    ["64MB"]=67108864
)

# 定义数据集配置
# 格式: "Name Provider Dir From To VolumeID"
DATASETS=(
    "AliCloud alicloud /mnt/d/alicloud_hot '2020-01-19 20:02' '2020-01-19 20:03' 225"
    "MSR1 msrc /mnt/e/msr-cambridge/MSR-Cambridge1 '2007-02-27 17:30' '2007-02-27 17:31' prn-0"
    "MSR2 msrc /mnt/e/msr-cambridge/MSR-Cambridge2 '2007-02-27 17:30' '2007-02-27 17:31' src1-0"
    "TBS tencent /mnt/d/tbs_hot '2018-10-09 17:11' '2018-10-09 17:12' 1662"
)

# 基础输出目录
BASE_OUT_DIR="out_dir_10days/vol"

echo "Starting batch analysis..."

# 遍历数据集
for dataset_info in "${DATASETS[@]}"; do
    # 解析数据集参数 (使用 eval 处理带空格的字符串)
    eval "params=($dataset_info)"
    NAME="${params[0]}"
    PROVIDER="${params[1]}"
    DIR="${params[2]}"
    FROM="${params[3]}"
    TO="${params[4]}"
    VOL_ID="${params[5]}"

    echo "----------------------------------------------------------------"
    echo "Processing Dataset: $NAME"
    echo "  Provider: $PROVIDER"
    echo "  Dir:      $DIR"
    echo "  Time:     $FROM to $TO"
    echo "  Volume:   $VOL_ID"
    echo "----------------------------------------------------------------"

    # 遍历 EC 策略
    for ec_conf in "${EC_CONFIGS[@]}"; do
        read -r DATA_BLOCKS PARITY_BLOCKS <<< "$ec_conf"
        EC_LABEL="${DATA_BLOCKS}+${PARITY_BLOCKS}"

        # 遍历块大小
        for size_label in "${!BLOCK_SIZES[@]}"; do
            SIZE_BYTES="${BLOCK_SIZES[$size_label]}"
            
            # 构建特定输出目录
            OUT_DIR="${BASE_OUT_DIR}/${NAME}/${EC_LABEL}/${size_label}"
            mkdir -p "$OUT_DIR"

            echo "  [Running] EC: $EC_LABEL, BlockSize: $size_label ($SIZE_BYTES bytes)"
            echo "    -> Output: $OUT_DIR"

            # 执行命令
            # 注意：使用 ./bin/ana 而不是 make run 以避免重复编译和参数传递问题
            ./bin/ana \
                -d "$DIR" \
                -provider "$PROVIDER" \
                -from "$FROM" \
                -to "$TO" \
                -target_vol "$VOL_ID" \
                -data_blocks "$DATA_BLOCKS" \
                -parity_blocks "$PARITY_BLOCKS" \
                -stripe_block_size "$SIZE_BYTES" \
                -o "$OUT_DIR" \
                > "${OUT_DIR}/analysis.log" 2>&1

            if [ $? -eq 0 ]; then
                echo "    [Success]"
            else
                echo "    [Failed] Check logs in ${OUT_DIR}/analysis.log"
                # 根据需求决定是否退出，这里选择继续执行下一个
                # exit 1 
            fi
        done
    done
done

echo "Batch analysis completed."
