# 给 AI 看的 prompt 集合

1. gendata.xml.tpl: 用于生成测试数据的 prompt 模板
2. gendata.xml: 跑 `make gen-prompt` 生成的实际 prompt 文件，**不要手动修改此文件**
3. generate.sh: 跑 `make gen-prompt` 时调用的生成 gendata.xml 的脚本
