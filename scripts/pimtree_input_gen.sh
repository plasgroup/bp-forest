#!/bin/bash
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
INPUT_DIR="$SCRIPT_DIR/../workload/input"
PIM_TREE_DIR="$SCRIPT_DIR/../external/PIM-tree"
PIM_TREE_BUILD_DIR="$PIM_TREE_DIR/build"
mkdir -p $INPUT_DIR
cd $PIM_TREE_DIR


# パラメータは以下の通り
dpu_nums=(64) #DPUの数(range queryの幅に相当？)
initial_element_num=(100000 100000) # 初期状態の要素の数
test_query_num=(10000 100000) # テスト時のqueryの数
test_query_skews=(0.9 0.99) # the skew of testing queries(=alpha)

# 初期状態の数, テスト時queryの数, skew配列の長さは揃える必要がある
parameter_length=${#initial_element_num[@]}
tasklet_num=12
pim_tree_build_conf_pattern=".NR_DPUS_*_NR_TASKLETS_*.conf"
echo $


for dpu_num in "${dpu_nums[@]}"; do

    # ビルド設定ファイルを確認し，存在して，同じ設定であればビルドをスキップ
    conf_file=$(find "$PIM_TREE_BUILD_DIR" -name "$pim_tree_build_conf_pattern" | head -n 1)
    if [ -n "$conf_file" ]; then
        # ファイルが見つかった場合
        # 正規表現で値を抽出
        if [[ $(basename "$conf_file") =~ \.NR_DPUS_([0-9]+)_NR_TASKLETS_([0-9]+)\.conf$ ]]; then
            NR_DPUS="${BASH_REMATCH[1]}"
            NR_TASKLETS="${BASH_REMATCH[2]}"

            if [ "$NR_DPUS" -eq "$dpu_num" ] && [ "$NR_TASKLETS" -eq "$tasklet_num" ]; then
                echo "NR_DPUS and NR_TASKLETS match the previous ones)."
            else
                # 過去のビルド設定を異なる時
                make clean
                make NR_DPUS=$dpu_num NR_TASKLETS=$tasklet_num
            fi
        fi
    else
        # ファイルが見つからない場合
        make clean
        make NR_DPUS=$dpu_num NR_TASKLETS=$tasklet_num
    fi


    for ((i=0; i<parameter_length; i++)); do
        initial_element_num=${initial_element_num[i]}
        test_query_num=${test_query_num[i]}
        test_query_skew=${test_query_skews[i]}
        init_file_name="init_dpu${dpu_num}_element${initial_element_num}.data"
        test_file_name="test_dpu${dpu_num}_query${test_query_num}_skew${test_query_skew}.data"

        build/pim_tree_host \
            -l $initial_element_num $test_query_num \
            --scan 1.0 \
            --predecessor 0 \
            --output "${INPUT_DIR}/${init_file_name}" "${INPUT_DIR}/${test_file_name}" \
            --alpha $test_query_skew
    done



done
