# 空売りデータノート

pipyが運営する、JPXの公表空売り残高ビューアと株式データの学習サイトです。
既存のGitHub Pages (`https://pipy.github.io/`) で配信する静的サイトです。

## コンテンツ

- 銘柄検索：証券コード・会社名・読み、キーボード選択、URL共有、履歴移動
- 一次資料付きの解説8本：空売り、信用倍率、費用、出来高、決算、増減率、調査手順
- 学習用計算ツール2種、用語集、出典・集計方法
- 運営者情報（pipy）、GitHub Issuesの問い合わせ窓口、プライバシー、免責事項

## 編集と確認

Python 3.9以上の標準ライブラリだけでページを生成できます。生成済みHTMLもGitに含める運用なので、GitHub Pages側のビルド設定変更は不要です。

```sh
python3 scripts/build_site.py
python3 scripts/validate_site.py
python3 -m http.server 8000 --bind 127.0.0.1
```

`http://127.0.0.1:8000/` で確認できます。

- 記事本文：`content/articles.json`
- 運営情報など：`content/pages.py`
- ホーム・計算ツール本文：`templates/`
- 共通レイアウト・canonical・サイトマップ：`scripts/build_site.py`
- 見た目・検索・計算：`assets/`

編集した記事の更新日も見直してください。現時点では生成スクリプト内の `UPDATED` を使用しています。公開URLを変更するときは `ORIGIN` と検証スクリプトのURLも変更します。

JSの回帰確認（Node.jsとjsdomが利用できる場合）：

```sh
node --check assets/viewer.js
node --check assets/calculators.js
node tests/calculators.cjs
node tests/viewer.cjs
```

`tests/viewer.cjs` はjsdomを利用します。サイト本体にはNode.jsやnpm依存はありません。

## データ更新

既存の `.github/workflows/update-shorts.yml` による平日17:10 JSTの更新設定を維持しています。取得・集計ロジックと既存JSONはこの改修では変更していません。更新遅延・停止はActionsで確認してください。

主表の差分は保存データとの差で、厳密な前営業日比を保証しません。内部状態 `reporting_lost` の判定は0.5%未満なので、画面では「公表基準未満」としています。基準日・継続行・履歴の制約は `methodology.html` に明記しています。

## 公開とAdSense申請

GitHub Pagesへの反映は公開リポジトリへのpushで行います。AdSenseへの再申請は別の操作です。公開時はGitHub側の `latest_shorts.json` を維持し、コンテンツの変更でデータを置き換えないでください。

1. pipy本人が記事内容、運営情報、問い合わせ窓口を確認する。
2. 通常のGitHub Pages公開手順で反映し、公開ページ・問い合わせリンク・記事・計算を確認する。
3. Search Consoleに `https://pipy.github.io/sitemap.xml` を登録し、クロール可能か確認する。
4. AdSense管理画面で対象サイト・所有権確認・不承認理由・ポリシーセンターを確認し、内容を改善したうえで審査を申請する。

既存のpublisher ID、ads.txt、Googleの所有権確認ファイルを保持しています。広告コードはホームと本文のある解説記事に設置し、計算ツール・運営情報・404ページには設置していません。自動広告の位置・量、対象地域で必要な認定CMP等の同意設定はAdSense管理画面側で確認してください。記事数や文字数だけで承認される保証はありません。

参照した公式案内：

- [AdSense向けのサイトの準備](https://support.google.com/adsense/answer/7299563?hl=ja)
- [承認されなかった場合](https://support.google.com/adsense/answer/81904?hl=ja)
- [プライバシーポリシーの必須コンテンツ](https://support.google.com/adsense/answer/1348695?hl=ja)
- [Googleパブリッシャー向けポリシー](https://support.google.com/adsense/answer/10502938?hl=ja)
