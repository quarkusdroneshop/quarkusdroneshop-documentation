# quarkusdroneshop-documentation

QuarkusDroneShop プロジェクト全体のドキュメントリポジトリである。各コンポーネントリポジトリの役割と、プロジェクト全体が目指す **Data Mesh 化** の目的・背景をまとめる。

- オリジナルサイト: [quarkusdroneshop.github.io](https://quarkusdroneshop.github.io)

## なぜ Data Mesh 化を目指すのか

QuarkusDroneShop はもともと、ドローン販売を題材にしたマイクロサービスのデモアプリケーションである。複数サイト(A/B/C)にまたがる複数のドメインサービス(quarkusdroneshop-web, quarkusdroneshop-counter, qdca10, qdca10pro, inventory, homeoffice-backend, homeoffice-ui 等)が存在し、それぞれが独立してデータを保有している。

### Before: ドメイン間の直接連携が抱える課題

サービスが増えるにつれ、ドメイン間のデータ連携が次のような形で個別最適化されていった。

- あるドメインが他ドメインの Kafka トピックを直接購読する
- あるドメインが他ドメインの DB を直接参照する
- スキーマが各ドメイン内でバラバラに管理され、互換性が保証されない
- 「どのデータがどこにあるか」を横断的に把握する手段がない
- メタデータ登録・スキーマ変更の追跡が手作業で、リードタイムが長い(数時間〜数日)

これは典型的な「分散モノリス」的な結合であり、ドメインが増えるほど連携コストが指数関数的に増大する。

### After: Data Mesh の原則に基づく再設計

QuarkusDroneShop の Data Mesh 化は、以下の原則に基づいて進めている。

1. **ドメイン間のデータ連携は必ずデータプロダクト経由にする** — ドメインサービスが他ドメインの生トピックや DB を直接購読・参照することを禁止し、[datamesh-dataproducts](https://github.com/nmushino/datamesh-dataproducts) が定義する公開データプロダクト(Kafka topic + Iceberg table + Trino view)経由でのみ連携する
2. **メタデータを Single Source of Truth にする** — OpenMetadata を中心に、スキーマ・データ契約・データ品質ルール・リネージをすべて一元管理する
3. **メタデータ管理を AI エージェントに担わせる** — [datamesh-ai-agent-platform](https://github.com/nmushino/datamesh-ai-agent-platform) が、スキーマ登録・メタデータ検索・データ品質管理といった定型作業を自動化し、人間は「判断」だけに集中する
4. **すべてのアクセスをガードレール付きの Gateway 経由にする** — AI エージェントや外部の MCP クライアントからのアクセスは、Red Hat Connectivity Link (MCP Gateway) + OPA によるガードレールを経由させ、認証・認可・レート制限・監査ログを統一的に効かせる

### 目標(KPI)

| 指標 | Before | After(目標) |
|---|---|---|
| メタデータ登録リードタイム | 数時間 | 5分 |
| データカタログ充足率 | 40% | 95% |
| データ品質スコア | 65% | 90% |
| スキーマ変更追跡率 | 30% | 100% |
| メタデータ登録工数 | — | 80% 削減 |
| データ資産の検索時間 | — | 90% 削減 |

## 全体アーキテクチャ

```
[Aサイト]                    [Bサイト]                [Cサイト]
quarkusdroneshop-web         qdca10                  homeoffice
quarkusdroneshop-counter     qdca10pro               homeoffice-ui
                             inventory

         ↕ Skupper (VAN)  ↕ KafkaMirrorMaker2 ↕

              ドメインの生イベントを直接購読しない
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │   datamesh-dataproducts (データプロダクト層)  │
        │   Flink (正規化・集計) → Iceberg → Trino (公開ビュー) │
        └─────────────────────────────────────┘
                          │
                          ▼
[RHDHサイト (オプション)]
  Red Hat Developer Hub (Backstage) ── 開発者ポータル
  OpenMetadata ── メタデータ Single Source of Truth
  datamesh-ai-agent-platform ── AI エージェントによるメタデータ自動管理
        │
        ▼
  MCP Gateway (Red Hat Connectivity Link)
  ── 認証(Keycloak) / 認可(OPA) / レート制限 / 監査ログ
```

## コンポーネント一覧

| リポジトリ | 役割 |
|---|---|
| [quarkusdroneshop-ansible](https://github.com/quarkusdroneshop/quarkusdroneshop-ansible) | OpenShift 環境構築の自動化(Ansible + シェルスクリプト)。3サイト構成、Skupper、Kafka、MCP Gateway(Connectivity Link)、データプロダクト基盤(Flink/Trino)のセットアップを一括管理する |
| [datamesh-dataproducts](https://github.com/nmushino/datamesh-dataproducts) | ドメイン間のデータ連携を仲介するデータプロダクト定義。各データプロダクトごとに Avro スキーマ・Flink SQL ジョブ・Iceberg テーブル DDL・Trino 公開ビューを持つ |
| [datamesh-ai-agent-platform](https://github.com/nmushino/datamesh-ai-agent-platform) | OpenShift AI 上で動作する AI エージェントプラットフォーム。OpenMetadata を中核に、スキーマ登録・メタデータ検索・ビジネスデータ登録・プラットフォーム運用を専門エージェントが自動化する |

### ドメインサービス(Aサイト/Bサイト/Cサイト)

各サイトで稼働する、Data Mesh 化以前から存在するドメインサービス本体。これらのサービス同士が生の Kafka トピックや DB を直接参照し合わないようにすることが、Data Mesh 化の出発点になっている。

| リポジトリ | サイト | 役割 |
|---|---|---|
| [quarkusdroneshop-counter](https://github.com/quarkusdroneshop/quarkusdroneshop-counter) | A | 注文調整サービス。Web から注文を受けて PostgreSQL に記録し、QDCA10/QDCA10pro へルーティング、製造完了を Web に通知する |
| [quarkusdroneshop-qdca10](https://github.com/quarkusdroneshop/quarkusdroneshop-qdca10) | B | ドリンク製造マイクロサービス(QDCA10モデル)。注文チケットを受けて製造ロジックを実行し、完了/欠品を通知する |
| [quarkusdroneshop-qdca10pro](https://github.com/quarkusdroneshop/quarkusdroneshop-qdca10pro) | B | ドリンク製造マイクロサービス(QDCA10Proモデル、エスプレッソ系担当)。QDCA10 と同様の役割 |
| [quarkusdroneshop-inventory](https://github.com/quarkusdroneshop/quarkusdroneshop-inventory) | B | 在庫管理サービス。製造サービスからの欠品(86'd)イベントを受け、補充コマンドを処理する |
| [quarkusdroneshop-homeoffice](https://github.com/quarkusdroneshop/quarkusdroneshop-homeoffice) | C | 複数店舗の売上・注文データを集計する GraphQL API バックエンド(homeoffice-ui 向け) |
| [quarkusdroneshop-homeoffice-ui](https://github.com/quarkusdroneshop/quarkusdroneshop-homeoffice-ui) | C | ホームオフィス向けダッシュボード UI。売上チャート・ライブ注文ボード・在庫アラートを表示する(React + TypeScript) |

いずれも Quarkus(homeoffice-ui のみ React)で実装されており、ドメイン間はこれまで Kafka トピックの直接購読で連携していた。この直接連携を [datamesh-dataproducts](https://github.com/nmushino/datamesh-dataproducts) 経由に置き換えていくこと、及び最新のコンポーネント、ライブラリを整理し、最終的には、Enterprise Nervous System を目指す。

詳細は [docs/components.md](docs/components.md) を参照。

## 参考

- [docs/components.md](docs/components.md) — 各コンポーネントの詳細説明
