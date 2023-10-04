# PIM_Masstree
## ディレクトリ
- dpu
  - DPUソースコード、ヘッダファイル(B+木の実装を含む)
- host
  - Host CPUソースコード、ヘッダファイル
- common
  - 共通のインクルードファイル
- data
  - 実験結果のcsvファイル
- graphs
  - csvファイルから描画したグラフ
- log
  - 実行時ログ
  
## パラメータの変更
- common/common.h
  - NR_DPUS
    - DPUの数(1~2560)
  - NUM_BATCH
    - バッチの数
  - NUM_REQUESTS
    - 挿入、検索の数
