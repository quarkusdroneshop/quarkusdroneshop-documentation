# コンポーネント詳細

QuarkusDroneShop の Data Mesh 化を構成する主要コンポーネントの詳細である。全体像は [README.md](../README.md) を参照。

## quarkusdroneshop-ansible

https://github.com/quarkusdroneshop/quarkusdroneshop-ansible

OpenShift 環境構築を自動化するリポジトリ。Ansible Playbook と複数のシェルスクリプト(`script/*.sh`)により、ミドルウェアのインストールからアプリデプロイまでを一括管理する。

### 役割

- **3サイト構成の基盤**: Aサイト(quarkusdroneshop-web, quarkusdroneshop-counter)/ Bサイト(qdca10, qdca10pro, inventory)/ Cサイト(homeoffice-backend, homeoffice-ui)それぞれに対して `./script/ocpdeploy.sh setup` で環境を構築する
- **サイト間連携**: Skupper(VAN)によるサイト間ネットワーク接続、Kafka MirrorMaker2 によるトピックミラーリング
- **データプロダクト基盤**: Flink Kubernetes Operator + Trino(Helm)のセットアップ、Apicurio へのスキーマ登録、Flink ジョブの投入(`ocpdeploy.sh dataproducts *`)
- **MCP Gateway**: Red Hat Connectivity Link(rhcl-operator)のインストールと、Gateway/HTTPRoute/AuthPolicy/RateLimitPolicy のデプロイ(`script/connectivitylink.sh`)
- **RHDH / OpenMetadata / AI Agent Platform**: オプションの RHDH サイトとして、Developer Hub・OpenMetadata・AI Agent Platform をデプロイする(`script/developer-hub.sh`, `script/openmetadata.sh`, `script/aiagent.sh`)

### なぜこの構成か

複数サイトにまたがるドメインサービスを個別に手作業で構築すると、環境ごとの設定差異(ドメイン名、証明書、トークン等)が蓄積し再現性が失われる。すべてのセットアップ手順をコード化することで、3サイトのどれであっても同じ手順で同じ結果を再現できるようにしている。

---

## datamesh-dataproducts

https://github.com/nmushino/datamesh-dataproducts

QuarkusDroneShop におけるドメイン間のイベント処理・データアクセスを、既存サービス間の直接連携(生 Kafka トピックの相互購読)から **データプロダクト経由** に切り替えるための基盤である。

### 方針

- **ドメインをまたぐデータ連携は、必ずデータプロダクト経由とする。** ドメインサービス(inventory / qdca10 / qdca10pro / homeoffice / web / counter 等)が他ドメインのトピックや DB を直接購読・参照することを禁止する
- **Kafka を中心に据える。** ドメインの生イベントを取り込み、加工した結果を Kafka トピック(および Iceberg テーブル)として再公開する
- **スキーマは Apicurio Service Registry で一元管理**し、互換性モード `BACKWARD`(可能な範囲で `BACKWARD_TRANSITIVE`)を強制する
- 採用技術: **Apache Flink**(Stream Processing、正規化・集計・join)、**Apache Iceberg**(Table Format、Time travel・監査対応)、**Trino**(SQL Query Engine、`dataproducts.*` スキーマの公開ビュー経由でのみアクセス許可)

### データプロダクト一覧

| データプロダクト | 内容 |
|---|---|
| [OrderEvents](https://github.com/nmushino/datamesh-dataproducts/tree/main/order-events) | 全ドメイン(Counter / QDCA10 / QDCA10pro / Homeoffice / Web)の注文関連イベントを正規化・統合した、データプロダクト基盤のハブ |
| [Real-time Sales Trends](https://github.com/nmushino/datamesh-dataproducts/tree/main/real-time-sales-trends) | OrderEvents を土台に、商品別の売上をリアルタイムウィンドウ集計 |
| [Drone Component Stock](https://github.com/nmushino/datamesh-dataproducts/tree/main/drone-component-stock) | inventory の在庫増減イベントから、部品(SKU)別の現在庫・在庫履歴を再構成 |
| [Inventory Analytics](https://github.com/nmushino/datamesh-dataproducts/tree/main/inventory-analytics) | Drone Component Stock と OrderEvents を突き合わせ、部品の消費速度・欠品リスクを分析(ドメイン内部の Postgres には一切アクセスしない) |
| [Assembly Lead Time QDCA10 / QDCA10pro](https://github.com/nmushino/datamesh-dataproducts/tree/main/assembly-lead-time-qdca10) | OrderEvents の明細ステータス変更イベントから、`PLACED → IN_PROGRESS → FULFILLED` の組立リードタイムを算出 |
| [Customer 360](https://github.com/nmushino/datamesh-dataproducts/tree/main/customer-360) | 複数ドメインのイベント・CDC を顧客IDで統合した単一プロファイルビュー。PII を含むため Trino 側でマスキング必須 |

各データプロダクトは `schema/`(Apicurio 登録用 Avro スキーマ)・`flink/`(Flink SQL Job)・`iceberg/`(Iceberg テーブル DDL)・`trino/`(Trino 公開ビュー定義)の4系統を持つ。

### 認証・認可

- **認証は Keycloak(RHBK)に一元化。** Flink / Apicurio / Trino それぞれに個別資格情報を作らず、サービスアカウントクライアント経由の OIDC / Client Credentials で統一する
- **認可は Trino 側のアクセス制御で行う。** Keycloak は「誰であるか」の確定のみを担い、行/カラムマスキングのような認可判断は Trino の File-based System Access Control(将来的に OPA への切り替えを想定)で行う。これは `datamesh-ai-agent-platform` のガードレール方式(OPA + Keycloak)と意図的に揃えている

### なぜこの設計か

ドメイン間の直接連携(生トピック相互購読、DB直接参照)は一見手軽だが、ドメインが増えるほど「誰が何を購読しているか」が把握不能になり、スキーマ変更の影響範囲がわからなくなる。データプロダクトという明示的な境界を設けることで、ドメインの内部実装(Postgresスキーマ、トピック名等)を変更してもデータプロダクトの公開契約(スキーマ・ビュー)さえ守れば他ドメインに影響しない、という疎結合を実現する。

---

## datamesh-ai-agent-platform

https://github.com/nmushino/datamesh-ai-agent-platform

OpenShift AI 上で動作する、OpenMetadata を中心としたエンタープライズ AI エージェントプラットフォーム。

### ビジョン

> 「AIエージェントが、データの意味を理解し、メタデータを自律的に管理する世界」

スキーマの登録、データカタログの更新、品質ルールの設定は専門知識を要する反復作業であり、データチームのボトルネックになっている。AI エージェントがその作業を担うことで、データエンジニアが本質的な価値創出に集中できる環境を提供する。

**Before(現状)**: 開発者がDBスキーマを変更 → 手動でOpenMetadataに登録(1-2時間) → ドキュメント更新(30分) → 品質ルール手動設定(1時間) → レビュー待ち(1-2日)。合計 3-4時間 + 待機時間。

**After(あるべき姿)**: 開発者がDBスキーマを変更 → AI Agentが自動検出・登録(5分) → メタデータ・品質ルール自動生成 → 通知・承認フロー開始。合計 5分、人間の作業ゼロ。

### 設計原則

1. **Agent-First** — 人間が介入するのは「判断」だけ。定型作業はすべてエージェントが実行する
2. **Tool-First** — エージェントの能力は Tool の集合で定義される。Tool は OpenMetadata API / Quarkus API をラップした再利用可能な関数群。AI は推論のみを行い、外部システムへのアクセスはすべて Tool 経由
3. **Metadata-First** — すべてのビジネスエンティティは OpenMetadata に登録され、AI が検索・参照できる状態を維持する(データ資産の Single Source of Truth)
4. **Human-in-the-Loop** — 重要な変更(本番DBスキーマ変更、データ削除等)は必ず人間の承認を経る

### エージェント構成

`agent/orchestrator/` の LangGraph `StateGraph` が意図分類してエージェントへルーティングし、破壊的操作は `human_approval_node` で一時停止して承認を待つ。

| エージェント | 役割 |
|---|---|
| Orchestrator | 意図分類・ルーティング(Tool は持たない) |
| Schema Agent | OpenMetadata スキーマ/トピック登録、Kafka トピック作成・削除 |
| Search Agent | データ資産検索、リネージ、データ品質メトリクス(読み取り専用) |
| Registration Agent | 顧客/BOM の登録・検索・更新(Business API 経由) |
| Platform Ops Agent | OpenShift/Git/Filesystem 運用(実装済みだがオーケストレーターへの接続は未完了) |

### ガードレールアーキテクチャ

```
User → MCP Gateway (Connectivity Link) → Input Guardrail → Planner Agent
     → Policy Engine (OPA + Keycloak) → Tool群 → Output Guardrail
     → Human Approval → Audit Log
```

- **認証は Keycloak に完全委任**。MCP Gateway(Red Hat Connectivity Link)の `AuthPolicy` が `issuerUrl` 指定だけでトークン署名検証を行い、Gateway 側に認証ロジックを自前実装しない
- **認可は OPA Policy Engine の責務として残す**(Tool policy はコード側)
- **コスト制御**は Gateway の `RateLimitPolicy` で下限の歯止めをかけ、詳細なコスト計算は `ai-policy-gateway` の cost limiter が担う
- MCP サーバーは `mcp-server/` として実装され、既存の LangChain Tool をラップして Model Context Protocol(streamable-http)で公開する

### なぜ AI エージェントに任せるのか

メタデータ管理はルールベースの自動化(スクリプト等)だけでは、命名揺れや文脈依存の判断(「このカラムはPIIか」「この変更は互換性を壊すか」)に対応しきれない。自然言語で意図を理解し、Tool を組み合わせて判断できる AI エージェントに定型的な判断を委譲しつつ、影響が大きい操作は Human-in-the-Loop で人間の承認を必須にすることで、自動化のスピードと安全性を両立させている。

## 目標KPI(datamesh-ai-agent-platform 由来)

| 指標 | 現状 | 目標 |
|---|---|---|
| メタデータ登録リードタイム | 4時間 | 5分 |
| データカタログ充足率 | 40% | 95% |
| データ品質スコア | 65% | 90% |
| スキーマ変更追跡率 | 30% | 100% |
