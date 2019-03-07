/*
 * SQLID ：chrg_strt_cust_info_all.sql
 * 処理名：課金開始会員情報_累積
 */
select 'POID,接続ID,入会日,脱会日,契約ステータス,契約フラグ,商材フラグ,申込日,開通日,オプション名コース名,転用承諾フラグ,商品ID,代理店ID,申込コード,氏名,生年月日,電話番号,初期契約解除' 
AS SELECT_RESULT from dual
union all
select  
        POID
||','|| 接続ID
||','|| 入会日
||','|| 脱会日
||','|| 契約ステータス
||','|| 契約フラグ
||','|| 商材フラグ
||','|| 申込日
||','|| 開通日
||','|| オプション名コース名
||','|| 転用承諾フラグ
||','|| 商品ID
||','|| 代理店ID
||','|| 申込コード
||','|| 氏名
||','|| 生年月日
||','|| 電話番号
||','|| 初期契約解除
as SELECT_RESULT
from(
--①ひかりコラボ関連コースの転用承諾番号が存在するコースオプション抽出ここから============================================================================================
--①－１コース抽出ここから------------------------------------------------------------------------------------------------------------------------------------------------
         select   cd.CNTRCT_ID as POID        --契約ID(POID)  
         ,        si.CNNCT_ID  as 接続ID                --接続ID（サービス情報（接続）と結合しているのでサービスIDがないならNull）
         ,        to_char(cn.CNTRCT_APPLY_DT,'FMYYYY/MM/DD') as 入会日         --入会日
         ,        to_char(cn.CNTRCT_END_DT,'FMYYYY/MM/DD') as 脱会日      --脱会日
         ,        vc.STS as 契約ステータス -- 契約ステータス
                  -- 契約フラグ（申込種別）
         ,        case when ch.APPLY_CLS = 0
                     then '新規申込'
                     when ch.APPLY_CLS = 1
                     then 'コース変更'
                  end as 契約フラグ
         ,       'コース' as 商材フラグ --商材フラグ
         ,       to_char(cd.CNTRCT_DT,'FMYYYY/MM/DD') as 申込日 --契約明細.契約申込日
         ,       to_char(cd.OPN_PRCSS_DT,'FMYYYY/MM/DD') as 開通日 -- 契約明細.課金期間from
         ,       replace(replace(mp.PRDCT_NM,'月額基本料',''),',','') as オプション名コース名 -- 商品名
         ,       '転用' as 転用承諾フラグ -- 転用承諾番号
         ,       cd.PRDCT_ID as 商品ID -- 商品コード
         ,       aa.AGNT_ID as 代理店ID -- 代理店ID
         ,       ch.APPLY_CD as 申込コード -- 申込コード
         ,       cc.CNTCT_NM_LST_NM || cc.CNTCT_NM_FST_NM as 氏名 --氏名
         ,       to_char(to_date(cu.BRTH_DT,'YYYY/MM/DD'),'FMYYYY/MM/DD')  as 生年月日              --生年月日
         ,       cct.CNTCT_TEL_NO as 電話番号 --連絡先電話番号
         ,       case when C.CUST_ID is not null then
                   '初期契約解除'
                 else
                   null
                 end as 初期契約解除 -- 初期契約解除
         from     M_PRDCT                   mp --商品マスタ
         ,        T_CNTRCT_DTL              cd --契約明細
         ,        T_SRVC_INF_CNNCT     si  --サービス情報（接続）
         ,        T_CNTRCT             cn  --契約
         ,        M_CUST               cu  --顧客
         ,        M_CUST_CNTCT        cc  --顧客連絡先
         ,        M_CUST_CNTCT_TEL_NO  cct --顧客連絡先電話番号
         ,        T_CRS_HSTRY          ch  --コース履歴
         ,        M_DL_PRDCT_LST       md  --取扱商品リスト
         ,        M_DL_UNT            mdu -- 取扱単位
         ,        T_AGNT_APPLY_USR    aa  -- 代理店一括申込ユーザ
         ,        V_C_CSR_STATUS      vc  -- csrweb顧客のステータスview
         ,        (select min(AGNT_APPLY_ID) as AGNT_APPLY_ID
                         ,CNTRCT_ID
                   from T_AGNT_APPLY_USR 
                   group by cntrct_id)
                  A -- 代理店一括申込ユーザの契約IDごとの代理店一括申込IDの最小値
         ,        (select CNTRCT_ID
                   from T_HIKARI_CLB_MNG
                   group by CNTRCT_ID
                   having count(HC_DVSN_ACCPT_NO) > 0
                   order by CNTRCT_ID)
                  B -- 光コラボ管理の転用承諾番号が存在する契約ID
         ,        (select CUST_ID, CNTRCT_ID
                   from M_CUST_CRRSPND_MEMO
                   where CUST_CRRSPND_MEMO like '%初期契約解除%'
                   group by CUST_ID, CNTRCT_ID)
                  C -- 顧客対応メモ 初期契約解除のみ
         where    
                 mp.PRDCT_ID         = cd.PRDCT_ID       -- 商品マスタ.商品ID ＝ 契約明細.商品ID
         and     si.SRVC_ID          = cd.SRVC_ID        -- サービス情報(接続).サービスID = 契約明細.サービスID
         and     cn.CNTRCT_ID        = cd.CNTRCT_ID      -- 契約.契約ID ＝ 契約明細.契約ID
         and     cn.CUST_ID          = cu.CUST_ID        -- 契約.顧客ID ＝顧客.顧客ID
         and     cn.CUST_ID          = cc.CUST_ID        -- 契約.顧客ID ＝顧客連絡先．顧客ID
         and     cn.CUST_ID          = cct.CUST_ID       -- 契約.顧客ID ＝顧客連絡先電話番号．顧客ID
         and     ch.CNTRCT_ID        = cd.CNTRCT_ID      -- コース履歴.契約ID ＝ 契約.契約ID
         and     mp.PRDCT_ID         = md.PRDCT_ID       -- 商品.商品ID ＝ 取扱商品リスト.商品ID
         and     cd.CNTRCT_ID        = A.CNTRCT_ID(+)    -- 契約明細.契約ID ＝ 代理店一括申込ユーザ.契約ID
         and     aa.AGNT_APPLY_ID(+) = A.AGNT_APPLY_ID   -- 代理店一括申込ユーザ.代理店一括申込ID ＝ 代理店一括申込ユーザ.契約IDごとの代理店一括申込IDの最小値
         and     B.CNTRCT_ID         = cd.CNTRCT_ID      -- 光コラボ管理.転用承諾番号が存在する契約ID ＝ 契約明細.契約ID
         and     si.CNNCT_ID         = aa.CNNCT_ID(+)    -- サービス情報（接続）.接続ID ＝ 代理店一括申込ユーザ.接続ID（代理店一括申込ユーザ.代理店一括申込ID重複のデータのため）
         and     ch.CRS_CHG_ID       = cd.CRS_CHG_ID     -- コース履歴.コース変更ID = 契約明細.コース変更ID
         and     md.DL_UNT_ID        = cd.DL_UNT_ID      -- 取扱商品リスト.取扱単位ID = 契約明細.取扱単位ID
         and     ch.DL_UNT_ID        = mdu.DL_UNT_ID      -- コース履歴.取扱単位ID  = 取扱単位.取扱単位ID
         and     cd.CNTRCT_ID        = vc.CNTRCT_ID      -- 契約明細.契約ID = csrweb顧客のステータスview.契約ID
         and     cn.CUST_ID          = C.CUST_ID(+)         -- 契約.顧客ID = 顧客対応メモ.顧客ID
         and     cn.CNTRCT_ID        = C.CNTRCT_ID(+)       -- 契約.契約ID = 顧客対応メモ.契約ID
         and     ch.DLT_FLG          = 0
         and     cn.DLT_FLG          = 0
         and     cu.DLT_FLG          = 0
         and     cd.DLT_FLG          = '0'
         and     mdu.DLT_FLG          = 0
         and     cd.CNTRCT_DTL_STS in (3,4,6)
         and     cd.OPN_PRCSS_DT   < add_months(trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year'),12)
         and     cd.OPN_PRCSS_DT   >= trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year')
         and     ch.CRS_CHG_CNCL_FLG = 0 --コース履歴.コース変更キャンセルフラグ 0 有効 1キャンセル
         and     cct.CNTCT_TEL_CLS   = 1 -- 電話番号区分 1 自宅
         and     md.PRI_CNNCT_PRDCT_FLG = 1 --主接続商品フラグ 1(コース)
         and     ch.APPLY_PRD_FROM < sysdate
         and     ch.APPLY_PRD_TO > sysdate
--①－１コース名抽出ここまで----------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL
--①－２オプション名抽出ここから------------------------------------------------------------------------------------------------------------------------------------------
        select   cd.CNTRCT_ID as POID          --契約ID(POID)
         ,        null as 接続ID                --接続ID（サービス情報（接続）と結合しているのでサービスIDがないならNull）
         ,        to_char(cn.CNTRCT_APPLY_DT,'FMYYYY/MM/DD')  as 入会日         --入会日
         ,        to_char(cn.CNTRCT_END_DT,'FMYYYY/MM/DD') as 脱会日      --脱会日
         ,        vc.STS as 契約ステータス -- 契約ステータス
                  -- 契約フラグ（申込種別）
         ,        case when ch.APPLY_CLS = 0
                     then '新規申込'
                     when ch.APPLY_CLS = 1
                     then 'コース変更'
                  end as 契約フラグ
         ,       'オプション' as 商材フラグ --商材フラグ
         ,       to_char(cd.CNTRCT_DT,'FMYYYY/MM/DD') as 申込日 --契約明細.契約申込日
         ,       to_char(cd.OPN_PRCSS_DT,'FMYYYY/MM/DD') as 開通日 -- 契約明細.課金期間from
         ,       replace(replace(mp.PRDCT_NM,'月額基本料',''),',','') as オプション名コース名 -- 商品名
         ,       '転用' as 転用承諾フラグ -- 転用承諾番号
         ,       cd.PRDCT_ID as 商品ID -- 商品コード
         ,       aa.AGNT_ID as 代理店ID -- 代理店ID
         ,       ch.APPLY_CD as 申込コード -- 申込コード
         ,       cc.CNTCT_NM_LST_NM || cc.CNTCT_NM_FST_NM as 氏名 --氏名
         ,       to_char(to_date(cu.BRTH_DT,'YYYY/MM/DD'),'FMYYYY/MM/DD')  as 生年月日              --生年月日
         ,       cct.CNTCT_TEL_NO as 電話番号 --連絡先電話番号
         ,       null as 初期契約解除 -- 初期契約解除
         from     M_PRDCT                   mp --商品マスタ
         ,        T_CNTRCT_DTL              cd --契約明細
         ,        T_CNTRCT             cn  --契約
         ,        M_CUST               cu  --顧客
         ,        M_CUST_CNTCT        cc  --顧客連絡先
         ,        M_CUST_CNTCT_TEL_NO  cct --顧客連絡先電話番号
         ,        T_CRS_HSTRY          ch  --コース履歴
         ,        M_DL_PRDCT_LST       md  --取扱商品リスト
         ,        M_DL_UNT            mdu -- 取扱単位
         ,        T_AGNT_APPLY_USR    aa  -- 代理店一括申込ユーザ
         ,        V_C_CSR_STATUS      vc  -- csrweb顧客のステータスview
         ,        (select min(AGNT_APPLY_ID) as AGNT_APPLY_ID
                         ,CNTRCT_ID
                   from T_AGNT_APPLY_USR 
                   group by cntrct_id)
                  A -- 代理店一括申込ユーザの契約IDごとの代理店一括申込IDの最小値
         ,        (select CNTRCT_ID
                   from T_HIKARI_CLB_MNG
                   group by CNTRCT_ID
                   having count(HC_DVSN_ACCPT_NO) > 0
                   order by CNTRCT_ID)
                  B -- 光コラボ管理の転用承諾番号が存在する契約ID
         where    
                 mp.PRDCT_ID         = cd.PRDCT_ID       -- 商品マスタ.商品ID ＝ 契約明細.商品ID
         and     cn.CNTRCT_ID        = cd.CNTRCT_ID      -- 契約.契約ID ＝ 契約明細.契約ID
         and     cn.CUST_ID          = cu.CUST_ID        -- 契約.顧客ID ＝顧客.顧客ID
         and     cn.CUST_ID          = cc.CUST_ID        -- 契約.顧客ID ＝顧客連絡先．顧客ID
         and     cn.CUST_ID          = cct.CUST_ID       -- 契約.顧客ID ＝顧客連絡先電話番号．顧客ID
         and     ch.CNTRCT_ID        = cd.CNTRCT_ID      -- コース履歴.契約ID ＝ 契約.契約ID
         and     mp.PRDCT_ID         = md.PRDCT_ID       -- 商品.商品ID ＝ 取扱商品リスト.商品ID
         and     aa.CNTRCT_ID(+)     = cd.CNTRCT_ID      -- 代理店一括申込ユーザ.契約ID ＝ 契約明細.契約ID
         and     cd.CNTRCT_ID        = A.CNTRCT_ID(+)    -- 契約明細.契約ID ＝ 代理店一括申込ユーザ.契約ID
         and     aa.AGNT_APPLY_ID(+) = A.AGNT_APPLY_ID   -- 代理店一括申込ユーザ.代理店一括申込ID ＝ 代理店一括申込ユーザ.契約IDごとの代理店一括申込IDの最小値
         and     B.CNTRCT_ID         = cd.CNTRCT_ID      -- 光コラボ管理.転用承諾番号が存在する契約ID ＝ 契約明細.契約ID
         and     ch.DL_UNT_ID        = mdu.DL_UNT_ID     -- コース履歴.取扱単位ID ＝ 取扱単位.取扱単位ID
         and     cd.DL_UNT_ID        = md.DL_UNT_ID      -- 契約明細.取扱単位ID ＝ 取扱商品リスト.取扱単位ID
         and     cd.CNTRCT_ID        = vc.CNTRCT_ID      -- 契約明細.契約ID = csrweb顧客のステータスview.契約ID
         and     ch.DLT_FLG          = 0
         and     cn.DLT_FLG          = 0
         and     cu.DLT_FLG          = 0
         and     cd.DLT_FLG          = '0'
         and     mdu.DLT_FLG          = 0
         and     cd.CNTRCT_DTL_STS in (3,4,6)
         and     cd.OPN_PRCSS_DT   < add_months(trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year'),12)
         and     cd.OPN_PRCSS_DT   >= trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year')
         and     ch.CRS_CHG_CNCL_FLG = 0 --コース履歴.コース変更キャンセルフラグ 0 有効 1キャンセル
         and     cct.CNTCT_TEL_CLS   = 1 -- 電話番号区分 1 自宅
         and     mp.PRDCT_ID not in(12030,10398,10617)-- 一部オプションは抽出しない（コンテンツ、WEBサービス、メールアドレス）
         and     mp.PRDCT_NM <> 'ユニバーサル料金'
         and     mp.PRDCT_NM not like '%従量料金%'
         and     mp.PRDCT_NM not like '%手数料%'
         and     mp.PRDCT_NM not like '%端末代金%'
         and     mp.PRDCT_NM not like '%hi-ho LTE typeD チャージ%'
         and     mp.PRDCT_NM not like '%SIM%'
         and     md.PRI_CNNCT_PRDCT_FLG = 0 --主接続商品フラグ 0(オプション)
         and     ch.APPLY_PRD_FROM < sysdate
         and     ch.APPLY_PRD_TO > sysdate
--①－２オプション名抽出ここまで------------------------------------------------------------------------------------------------------------------------------------------
--①ひかりコラボ関連コースの転用承諾番号が存在するコースオプション抽出ここまで============================================================================================
UNION ALL
--②ひかりコラボ関連コースの転用承諾番号が存在しないコースオプション抽出ここから==========================================================================================
--②－１コース名抽出ここから----------------------------------------------------------------------------------------------------------------------------------------------
         select   cd.CNTRCT_ID as POID        --契約ID(POID)  
         ,        si.CNNCT_ID  as 接続ID                --接続ID（サービス情報（接続）と結合しているのでサービスIDがないならNull）
         ,        to_char(cn.CNTRCT_APPLY_DT,'FMYYYY/MM/DD') as 入会日         --入会日
         ,        to_char(cn.CNTRCT_END_DT,'FMYYYY/MM/DD') as 脱会日      --脱会日
         ,        vc.STS as 契約ステータス -- 契約ステータス
                  -- 契約フラグ（申込種別）
         ,        case when ch.APPLY_CLS = 0
                     then '新規申込'
                     when ch.APPLY_CLS = 1
                     then 'コース変更'
                  end as 契約フラグ
         ,       'コース' as 商材フラグ --商材フラグ
         ,       to_char(cd.CNTRCT_DT,'FMYYYY/MM/DD') as 申込日 --契約明細.契約申込日
         ,       to_char(cd.OPN_PRCSS_DT,'FMYYYY/MM/DD') as 開通日 -- 契約明細.課金期間from
         ,       replace(replace(mp.PRDCT_NM,'月額基本料',''),',','') as オプション名コース名 -- 商品名
         ,       null as 転用承諾フラグ -- 転用承諾番号
         ,       cd.PRDCT_ID as 商品ID -- 商品コード
         ,       aa.AGNT_ID as 代理店ID -- 代理店ID
         ,       ch.APPLY_CD as 申込コード -- 申込コード
         ,       cc.CNTCT_NM_LST_NM || cc.CNTCT_NM_FST_NM as 氏名 --氏名
         ,       to_char(to_date(cu.BRTH_DT,'YYYY/MM/DD'),'FMYYYY/MM/DD')  as 生年月日              --生年月日
         ,       cct.CNTCT_TEL_NO as 電話番号 --連絡先電話番号
         ,       case when C.CUST_ID is not null then
                   '初期契約解除'
                 else
                   null
                 end as 初期契約解除 -- 初期契約解除
         from     M_PRDCT                   mp --商品マスタ
         ,        T_CNTRCT_DTL              cd --契約明細
         ,        T_SRVC_INF_CNNCT     si  --サービス情報（接続）
         ,        T_CNTRCT             cn  --契約
         ,        M_CUST               cu  --顧客
         ,        M_CUST_CNTCT        cc  --顧客連絡先
         ,        M_CUST_CNTCT_TEL_NO  cct --顧客連絡先電話番号
         ,        T_CRS_HSTRY          ch  --コース履歴
         ,        M_DL_PRDCT_LST       md  --取扱商品リスト
         ,        M_DL_UNT            mdu -- 取扱単位
         ,        T_AGNT_APPLY_USR    aa  -- 代理店一括申込ユーザ
         ,        V_C_CSR_STATUS      vc  -- csrweb顧客のステータスview
         ,        (select min(AGNT_APPLY_ID) as AGNT_APPLY_ID
                         ,CNTRCT_ID
                   from T_AGNT_APPLY_USR 
                   group by cntrct_id)
                  A -- 代理店一括申込ユーザの契約IDごとの代理店一括申込IDの最小値
         ,        (select CNTRCT_ID
                   from T_HIKARI_CLB_MNG
                   group by CNTRCT_ID
                   having count(HC_DVSN_ACCPT_NO) = 0
                   order by CNTRCT_ID)
                  B -- 光コラボ管理の転用承諾番号が存在する契約ID
         ,        (select CUST_ID, CNTRCT_ID
                   from M_CUST_CRRSPND_MEMO
                   where CUST_CRRSPND_MEMO like '%初期契約解除%'
                   group by CUST_ID, CNTRCT_ID)
                  C -- 顧客対応メモ 初期契約解除のみ
         where    
                 mp.PRDCT_ID         = cd.PRDCT_ID       -- 商品マスタ.商品ID ＝ 契約明細.商品ID
         and     si.SRVC_ID          = cd.SRVC_ID        -- サービス情報(接続).サービスID = 契約明細.サービスID
         and     cn.CNTRCT_ID        = cd.CNTRCT_ID      -- 契約.契約ID ＝ 契約明細.契約ID
         and     cn.CUST_ID          = cu.CUST_ID        -- 契約.顧客ID ＝顧客.顧客ID
         and     cn.CUST_ID          = cc.CUST_ID        -- 契約.顧客ID ＝顧客連絡先．顧客ID
         and     cn.CUST_ID          = cct.CUST_ID       -- 契約.顧客ID ＝顧客連絡先電話番号．顧客ID
         and     ch.CNTRCT_ID        = cd.CNTRCT_ID      -- コース履歴.契約ID ＝ 契約.契約ID
         and     mp.PRDCT_ID         = md.PRDCT_ID       -- 商品.商品ID ＝ 取扱商品リスト.商品ID
         and     cd.CNTRCT_ID        = A.CNTRCT_ID(+)    -- 契約明細.契約ID ＝ 代理店一括申込ユーザ.契約ID
         and     aa.AGNT_APPLY_ID(+) = A.AGNT_APPLY_ID   -- 代理店一括申込ユーザ.代理店一括申込ID ＝ 代理店一括申込ユーザ.契約IDごとの代理店一括申込IDの最小値
         and     B.CNTRCT_ID         = cd.CNTRCT_ID      -- 光コラボ管理.転用承諾番号が存在しない契約ID ＝ 契約明細.契約ID
         and     si.CNNCT_ID         = aa.CNNCT_ID(+)    -- サービス情報（接続）.接続ID ＝ 代理店一括申込ユーザ.接続ID（代理店一括申込ユーザ.代理店一括申込ID重複のデータのため）
         and     ch.CRS_CHG_ID       = cd.CRS_CHG_ID     -- コース履歴.コース変更ID = 契約明細.コース変更ID
         and     md.DL_UNT_ID        = cd.DL_UNT_ID      -- 取扱商品リスト.取扱単位ID = 契約明細.取扱単位ID
         and     ch.DL_UNT_ID        = mdu.DL_UNT_ID      -- コース履歴.取扱単位ID  = 取扱単位.取扱単位ID
         and     cd.CNTRCT_ID        = vc.CNTRCT_ID      -- 契約明細.契約ID = csrweb顧客のステータスview.契約ID
         and     cn.CUST_ID          = C.CUST_ID(+)         -- 契約.顧客ID = 顧客対応メモ.顧客ID
         and     cn.CNTRCT_ID        = C.CNTRCT_ID(+)       -- 契約.契約ID = 顧客対応メモ.契約ID
         and     ch.DLT_FLG          = 0
         and     cn.DLT_FLG          = 0
         and     cu.DLT_FLG          = 0
         and     cd.DLT_FLG          = '0'
         and     mdu.DLT_FLG          = 0
         and     cd.CNTRCT_DTL_STS in (3,4,6)
         and     cd.OPN_PRCSS_DT   < add_months(trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year'),12)
         and     cd.OPN_PRCSS_DT   >= trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year')
         and     ch.CRS_CHG_CNCL_FLG = 0 --コース履歴.コース変更キャンセルフラグ 0 有効 1キャンセル
         and     cct.CNTCT_TEL_CLS   = 1 -- 電話番号区分 1 自宅
         and     md.PRI_CNNCT_PRDCT_FLG = 1 --主接続商品フラグ 1(コース)
         and     ch.APPLY_PRD_FROM < sysdate
         and     ch.APPLY_PRD_TO > sysdate
--②－１コース名抽出ここまで----------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL
--②－２オプション名抽出ここから------------------------------------------------------------------------------------------------------------------------------------------
        select   cd.CNTRCT_ID as POID          --契約ID(POID)
         ,        null as 接続ID                --接続ID（サービス情報（接続）と結合しているのでサービスIDがないならNull）
         ,        to_char(cn.CNTRCT_APPLY_DT,'FMYYYY/MM/DD')  as 入会日         --入会日
         ,        to_char(cn.CNTRCT_END_DT,'FMYYYY/MM/DD') as 脱会日      --脱会日
         ,        vc.STS as 契約ステータス -- 契約ステータス
                  -- 契約フラグ（申込種別）
         ,        case when ch.APPLY_CLS = 0
                     then '新規申込'
                     when ch.APPLY_CLS = 1
                     then 'コース変更'
                  end as 契約フラグ
         ,       'オプション' as 商材フラグ --商材フラグ
         ,       to_char(cd.CNTRCT_DT,'FMYYYY/MM/DD') as 申込日 --契約明細.契約申込日
         ,       to_char(cd.OPN_PRCSS_DT,'FMYYYY/MM/DD') as 開通日 -- 契約明細.課金期間from
         ,       replace(replace(mp.PRDCT_NM,'月額基本料',''),',','') as オプション名コース名 -- 商品名
         ,       null as 転用承諾フラグ -- 転用承諾番号
         ,       cd.PRDCT_ID as 商品ID -- 商品コード
         ,       aa.AGNT_ID as 代理店ID -- 代理店ID
         ,       ch.APPLY_CD as 申込コード -- 申込コード
         ,       cc.CNTCT_NM_LST_NM || cc.CNTCT_NM_FST_NM as 氏名 --氏名
         ,       to_char(to_date(cu.BRTH_DT,'YYYY/MM/DD'),'FMYYYY/MM/DD')  as 生年月日              --生年月日
         ,       cct.CNTCT_TEL_NO as 電話番号 --連絡先電話番号
         ,       null as 初期契約解除 -- 初期契約解除
         from     M_PRDCT                   mp --商品マスタ
         ,        T_CNTRCT_DTL              cd --契約明細
         ,        T_CNTRCT             cn  --契約
         ,        M_CUST               cu  --顧客
         ,        M_CUST_CNTCT        cc  --顧客連絡先
         ,        M_CUST_CNTCT_TEL_NO  cct --顧客連絡先電話番号
         ,        T_CRS_HSTRY          ch  --コース履歴
         ,        M_DL_PRDCT_LST       md  --取扱商品リスト
         ,        M_DL_UNT            mdu -- 取扱単位
         ,        T_AGNT_APPLY_USR    aa  -- 代理店一括申込ユーザ
         ,        V_C_CSR_STATUS      vc  -- csrweb顧客のステータスview
         ,        (select min(AGNT_APPLY_ID) as AGNT_APPLY_ID
                         ,CNTRCT_ID
                   from T_AGNT_APPLY_USR 
                   group by cntrct_id)
                  A -- 代理店一括申込ユーザの契約IDごとの代理店一括申込IDの最小値
         ,        (select CNTRCT_ID
                   from T_HIKARI_CLB_MNG
                   group by CNTRCT_ID
                   having count(HC_DVSN_ACCPT_NO) = 0
                   order by CNTRCT_ID)
                  B -- 光コラボ管理の転用承諾番号が存在しない契約ID
         where    
                 mp.PRDCT_ID         = cd.PRDCT_ID       -- 商品マスタ.商品ID ＝ 契約明細.商品ID
         and     cn.CNTRCT_ID        = cd.CNTRCT_ID      -- 契約.契約ID ＝ 契約明細.契約ID
         and     cn.CUST_ID          = cu.CUST_ID        -- 契約.顧客ID ＝顧客.顧客ID
         and     cn.CUST_ID          = cc.CUST_ID        -- 契約.顧客ID ＝顧客連絡先．顧客ID
         and     cn.CUST_ID          = cct.CUST_ID       -- 契約.顧客ID ＝顧客連絡先電話番号．顧客ID
         and     ch.CNTRCT_ID        = cd.CNTRCT_ID      -- コース履歴.契約ID ＝ 契約.契約ID
         and     mp.PRDCT_ID         = md.PRDCT_ID       -- 商品.商品ID ＝ 取扱商品リスト.商品ID
         and     aa.CNTRCT_ID(+)     = cd.CNTRCT_ID      -- 代理店一括申込ユーザ.契約ID ＝ 契約明細.契約ID
         and     cd.CNTRCT_ID        = A.CNTRCT_ID(+)    -- 契約明細.契約ID ＝ 代理店一括申込ユーザ.契約ID
         and     aa.AGNT_APPLY_ID(+) = A.AGNT_APPLY_ID   -- 代理店一括申込ユーザ.代理店一括申込ID ＝ 代理店一括申込ユーザ.契約IDごとの代理店一括申込IDの最小値
         and     B.CNTRCT_ID         = cd.CNTRCT_ID      -- 光コラボ管理.転用承諾番号が存在しない契約ID ＝ 契約明細.契約ID
         and     ch.DL_UNT_ID        = mdu.DL_UNT_ID     -- コース履歴.取扱単位ID ＝ 取扱単位.取扱単位ID
         and     cd.DL_UNT_ID        = md.DL_UNT_ID      -- 契約明細.取扱単位ID ＝ 取扱商品リスト.取扱単位ID
         and     cd.CNTRCT_ID        = vc.CNTRCT_ID      -- 契約明細.契約ID = csrweb顧客のステータスview.契約ID
         and     ch.DLT_FLG          = 0
         and     cn.DLT_FLG          = 0
         and     cu.DLT_FLG          = 0
         and     cd.DLT_FLG          = '0'
         and     mdu.DLT_FLG          = 0
         and     cd.CNTRCT_DTL_STS in (3,4,6)
         and     cd.OPN_PRCSS_DT   < add_months(trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year'),12)
         and     cd.OPN_PRCSS_DT   >= trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year')
         and     ch.CRS_CHG_CNCL_FLG = 0 --コース履歴.コース変更キャンセルフラグ 0 有効 1キャンセル
         and     cct.CNTCT_TEL_CLS   = 1 -- 電話番号区分 1 自宅
         and     mp.PRDCT_ID not in (12030,10398,10617) -- 一部オプションは抽出しない（コンテンツ、WEBサービス、メールアドレス）
         and     mp.PRDCT_NM <> 'ユニバーサル料金'
         and     mp.PRDCT_NM not like '%従量料金%'
         and     mp.PRDCT_NM not like '%手数料%'
         and     mp.PRDCT_NM not like '%端末代金%'
         and     mp.PRDCT_NM not like '%hi-ho LTE typeD チャージ%'
         and     mp.PRDCT_NM not like '%SIM%'
         and     md.PRI_CNNCT_PRDCT_FLG = 0 --主接続商品フラグ 0(オプション)
         and     ch.APPLY_PRD_FROM < sysdate
         and     ch.APPLY_PRD_TO > sysdate
--②－２オプション名抽出ここまで------------------------------------------------------------------------------------------------------------------------------------------
--②ひかりコラボ関連コースの転用承諾番号が存在しないコースオプション抽出ここまで==========================================================================================
UNION ALL
--③光コラボ関連以外のコースオプション抽出ここから========================================================================================================================
--③－１コース名抽出ここから----------------------------------------------------------------------------------------------------------------------------------------------
         select   *
         from
         (
             select   cd.CNTRCT_ID as POID        --契約ID(POID)  
             ,        si.CNNCT_ID  as 接続ID                --接続ID（サービス情報（接続）と結合しているのでサービスIDがないならNull）
             ,        to_char(cn.CNTRCT_APPLY_DT,'FMYYYY/MM/DD') as 入会日         --入会日
             ,        to_char(cn.CNTRCT_END_DT,'FMYYYY/MM/DD') as 脱会日      --脱会日
             ,        vc.STS as 契約ステータス -- 契約ステータス
                      -- 契約フラグ（申込種別）
             ,        case when ch.APPLY_CLS = 0
                         then '新規申込'
                         when ch.APPLY_CLS = 1
                         then 'コース変更'
                      end as 契約フラグ
             ,       'コース' as 商材フラグ --商材フラグ
             ,       to_char(cd.CNTRCT_DT,'FMYYYY/MM/DD') as 申込日 --契約明細.契約申込日
             ,       to_char(cd.CHRG_PRD_FROM,'FMYYYY/MM/DD') as 開通日 -- 契約明細.課金期間from
             ,       replace(replace(mp.PRDCT_NM,'月額基本料',''),',','') as オプション名コース名 -- 商品名
             ,       null as 転用承諾フラグ -- 転用承諾番号
             ,       cd.PRDCT_ID as 商品ID -- 商品コード
             ,       aa.AGNT_ID as 代理店ID -- 代理店ID
             ,       ch.APPLY_CD as 申込コード -- 申込コード
             ,       cc.CNTCT_NM_LST_NM || cc.CNTCT_NM_FST_NM as 氏名 --氏名
             ,       to_char(to_date(cu.BRTH_DT,'YYYY/MM/DD'),'FMYYYY/MM/DD')  as 生年月日              --生年月日
             ,       cct.CNTCT_TEL_NO as 電話番号 --連絡先電話番号
             ,       case when C.CUST_ID is not null then
                       '初期契約解除'
                     else
                       null
                     end as 初期契約解除 -- 初期契約解除
             from     M_PRDCT                   mp --商品マスタ
             ,        T_CNTRCT_DTL              cd --契約明細
             ,        T_SRVC_INF_CNNCT     si  --サービス情報（接続）
             ,        T_CNTRCT             cn  --契約
             ,        M_CUST               cu  --顧客
             ,        M_CUST_CNTCT        cc  --顧客連絡先
             ,        M_CUST_CNTCT_TEL_NO  cct --顧客連絡先電話番号
             ,        T_CRS_HSTRY          ch  --コース履歴
             ,        M_DL_PRDCT_LST       md  --取扱商品リスト
             ,        M_DL_UNT            mdu -- 取扱単位
             ,        T_AGNT_APPLY_USR    aa  -- 代理店一括申込ユーザ
             ,        V_C_CSR_STATUS      vc  -- csrweb顧客のステータスview
             ,        (select min(AGNT_APPLY_ID) as AGNT_APPLY_ID
                             ,CNTRCT_ID
                       from T_AGNT_APPLY_USR 
                       group by cntrct_id)
                      A -- 代理店一括申込ユーザの契約IDごとの代理店一括申込IDの最小値
             ,        (select CUST_ID, CNTRCT_ID
                       from M_CUST_CRRSPND_MEMO
                       where CUST_CRRSPND_MEMO like '%初期契約解除%'
                       group by CUST_ID, CNTRCT_ID)
                      C -- 顧客対応メモ 初期契約解除のみ
             where    
                     mp.PRDCT_ID         = cd.PRDCT_ID       -- 商品マスタ.商品ID ＝ 契約明細.商品ID
             and     si.SRVC_ID          = cd.SRVC_ID        -- サービス情報(接続).サービスID = 契約明細.サービスID
             and     cn.CNTRCT_ID        = cd.CNTRCT_ID      -- 契約.契約ID ＝ 契約明細.契約ID
             and     cn.CUST_ID          = cu.CUST_ID        -- 契約.顧客ID ＝顧客.顧客ID
             and     cn.CUST_ID          = cc.CUST_ID        -- 契約.顧客ID ＝顧客連絡先．顧客ID
             and     cn.CUST_ID          = cct.CUST_ID       -- 契約.顧客ID ＝顧客連絡先電話番号．顧客ID
             and     ch.CNTRCT_ID        = cd.CNTRCT_ID      -- コース履歴.契約ID ＝ 契約.契約ID
             and     mp.PRDCT_ID         = md.PRDCT_ID       -- 商品.商品ID ＝ 取扱商品リスト.商品ID
             and     cd.CNTRCT_ID        = A.CNTRCT_ID(+)    -- 契約明細.契約ID ＝ 代理店一括申込ユーザ.契約ID
             and     aa.AGNT_APPLY_ID(+) = A.AGNT_APPLY_ID   -- 代理店一括申込ユーザ.代理店一括申込ID ＝ 代理店一括申込ユーザ.契約IDごとの代理店一括申込IDの最小値
             and     si.CNNCT_ID         = aa.CNNCT_ID(+)    -- サービス情報（接続）.接続ID ＝ 代理店一括申込ユーザ.接続ID（代理店一括申込ユーザ.代理店一括申込ID重複のデータのため）
             and     ch.CRS_CHG_ID       = cd.CRS_CHG_ID     -- コース履歴.コース変更ID = 契約明細.コース変更ID
             and     md.DL_UNT_ID        = cd.DL_UNT_ID      -- 取扱商品リスト.取扱単位ID = 契約明細.取扱単位ID
             and     ch.DL_UNT_ID        = mdu.DL_UNT_ID      -- コース履歴.取扱単位ID  = 取扱単位.取扱単位ID
             and     cd.CNTRCT_ID        = vc.CNTRCT_ID      -- 契約明細.契約ID = csrweb顧客のステータスview.契約ID
             and     cn.CUST_ID          = C.CUST_ID(+)         -- 契約.顧客ID = 顧客対応メモ.顧客ID
             and     cn.CNTRCT_ID        = C.CNTRCT_ID(+)       -- 契約.契約ID = 顧客対応メモ.契約ID
             and     ch.DLT_FLG          = 0
             and     cn.DLT_FLG          = 0
             and     cu.DLT_FLG          = 0
             and     cd.DLT_FLG          = '0'
             and     mdu.DLT_FLG          = 0
             and     cd.CNTRCT_DTL_STS in (3,4,6)
             and     cd.CHRG_PRD_FROM   < add_months(trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year'),12)
             and     cd.CHRG_PRD_FROM   >= trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year')
             and     ch.CRS_CHG_CNCL_FLG = 0 --コース履歴.コース変更キャンセルフラグ 0 有効 1キャンセル
             and     cct.CNTCT_TEL_CLS   = 1 -- 電話番号区分 1 自宅
             and     md.PRI_CNNCT_PRDCT_FLG = 1 --主接続商品フラグ 1(コース)
             and     ch.APPLY_PRD_FROM < sysdate
             and     ch.APPLY_PRD_TO > sysdate
         ) AA
         where NOT EXISTS (SELECT 1 FROM T_HIKARI_CLB_MNG BB WHERE AA.POID = BB.CNTRCT_ID) --光関連のデータは抽出しない
--③－１コース名抽出ここまで----------------------------------------------------------------------------------------------------------------------------------------------
UNION ALL
--③－２オプション名抽出ここから------------------------------------------------------------------------------------------------------------------------------------------
        select *
        from
        (
            select   cd.CNTRCT_ID as POID          --契約ID(POID)
             ,        null as 接続ID                --接続ID（サービス情報（接続）と結合しているのでサービスIDがないならNull）
             ,        to_char(cn.CNTRCT_APPLY_DT,'FMYYYY/MM/DD')  as 入会日         --入会日
             ,        to_char(cn.CNTRCT_END_DT,'FMYYYY/MM/DD') as 脱会日      --脱会日
             ,        vc.STS as 契約ステータス -- 契約ステータス
                      -- 契約フラグ（申込種別）
             ,        case when ch.APPLY_CLS = 0
                         then '新規申込'
                         when ch.APPLY_CLS = 1
                         then 'コース変更'
                      end as 契約フラグ
             ,       'オプション' as 商材フラグ --商材フラグ
             ,       to_char(cd.CNTRCT_DT,'FMYYYY/MM/DD') as 申込日 --契約明細.契約申込日
             ,       to_char(cd.CHRG_PRD_FROM,'FMYYYY/MM/DD') as 開通日 -- 契約明細.課金期間from
             ,       replace(replace(mp.PRDCT_NM,'月額基本料',''),',','') as オプション名コース名 -- 商品名
             ,       null as 転用承諾フラグ -- 転用承諾番号
             ,       cd.PRDCT_ID as 商品ID -- 商品コード
             ,       aa.AGNT_ID as 代理店ID -- 代理店ID
             ,       ch.APPLY_CD as 申込コード -- 申込コード
             ,       cc.CNTCT_NM_LST_NM || cc.CNTCT_NM_FST_NM as 氏名 --氏名
             ,       to_char(to_date(cu.BRTH_DT,'YYYY/MM/DD'),'FMYYYY/MM/DD')  as 生年月日              --生年月日
             ,       cct.CNTCT_TEL_NO as 電話番号 --連絡先電話番号
             ,       null as 初期契約解除 -- 初期契約解除
             from     M_PRDCT                   mp --商品マスタ
             ,        T_CNTRCT_DTL              cd --契約明細
             ,        T_CNTRCT             cn  --契約
             ,        M_CUST               cu  --顧客
             ,        M_CUST_CNTCT        cc  --顧客連絡先
             ,        M_CUST_CNTCT_TEL_NO  cct --顧客連絡先電話番号
             ,        T_CRS_HSTRY          ch  --コース履歴
             ,        M_DL_PRDCT_LST       md  --取扱商品リスト
             ,        M_DL_UNT            mdu -- 取扱単位
             ,        T_AGNT_APPLY_USR    aa  -- 代理店一括申込ユーザ
             ,        V_C_CSR_STATUS      vc  -- csrweb顧客のステータスview
             ,        (select min(AGNT_APPLY_ID) as AGNT_APPLY_ID
                             ,CNTRCT_ID
                       from T_AGNT_APPLY_USR 
                       group by cntrct_id)
                      A -- 代理店一括申込ユーザの契約IDごとの代理店一括申込IDの最小値
             where    
                     mp.PRDCT_ID         = cd.PRDCT_ID       -- 商品マスタ.商品ID ＝ 契約明細.商品ID
             and     cn.CNTRCT_ID        = cd.CNTRCT_ID      -- 契約.契約ID ＝ 契約明細.契約ID
             and     cn.CUST_ID          = cu.CUST_ID        -- 契約.顧客ID ＝顧客.顧客ID
             and     cn.CUST_ID          = cc.CUST_ID        -- 契約.顧客ID ＝顧客連絡先．顧客ID
             and     cn.CUST_ID          = cct.CUST_ID       -- 契約.顧客ID ＝顧客連絡先電話番号．顧客ID
             and     ch.CNTRCT_ID        = cd.CNTRCT_ID      -- コース履歴.契約ID ＝ 契約.契約ID
             and     mp.PRDCT_ID         = md.PRDCT_ID       -- 商品.商品ID ＝ 取扱商品リスト.商品ID
             and     aa.CNTRCT_ID(+)     = cd.CNTRCT_ID      -- 代理店一括申込ユーザ.契約ID ＝ 契約明細.契約ID
             and     cd.CNTRCT_ID        = A.CNTRCT_ID(+)    -- 契約明細.契約ID ＝ 代理店一括申込ユーザ.契約ID
             and     aa.AGNT_APPLY_ID(+) = A.AGNT_APPLY_ID   -- 代理店一括申込ユーザ.代理店一括申込ID ＝ 代理店一括申込ユーザ.契約IDごとの代理店一括申込IDの最小値
             and     ch.DL_UNT_ID        = mdu.DL_UNT_ID     -- コース履歴.取扱単位ID ＝ 取扱単位.取扱単位ID
             and     cd.DL_UNT_ID        = md.DL_UNT_ID      -- 契約明細.取扱単位ID ＝ 取扱商品リスト.取扱単位ID
             and     cd.CNTRCT_ID        = vc.CNTRCT_ID      -- 契約明細.契約ID = csrweb顧客のステータスview.契約ID
             and     ch.DLT_FLG          = 0
             and     cn.DLT_FLG          = 0
             and     cu.DLT_FLG          = 0
             and     cd.DLT_FLG          = '0'
             and     mdu.DLT_FLG          = 0
             and     cd.CNTRCT_DTL_STS in (3,4,6)
             and     cd.CHRG_PRD_FROM   < add_months(trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year'),12)
             and     cd.CHRG_PRD_FROM   >= trunc(to_date(/*sqlGtCndt1*/, 'YYYY'), 'year')
             and     ch.CRS_CHG_CNCL_FLG = 0 --コース履歴.コース変更キャンセルフラグ 0 有効 1キャンセル
             and     cct.CNTCT_TEL_CLS   = 1 -- 電話番号区分 1 自宅
             and     mp.PRDCT_ID not in (12030,10398,10617) -- 一部オプションは抽出しない（コンテンツ、WEBサービス、メールアドレス）
             and     mp.PRDCT_NM <> 'ユニバーサル料金'
             and     mp.PRDCT_NM not like '%従量料金%'
             and     mp.PRDCT_NM not like '%手数料%'
             and     mp.PRDCT_NM not like '%端末代金%'
             and     mp.PRDCT_NM not like '%hi-ho LTE typeD チャージ%'
             and     mp.PRDCT_NM not like '%SIM%'
             and     md.PRI_CNNCT_PRDCT_FLG = 0 --主接続商品フラグ 0(オプション)
             and     ch.APPLY_PRD_FROM < sysdate
             and     ch.APPLY_PRD_TO > sysdate
         ) AA
         where NOT EXISTS (SELECT 1 FROM T_HIKARI_CLB_MNG BB WHERE AA.POID = BB.CNTRCT_ID) --光関連のデータは抽出しない
--③－２オプション名抽出ここまで------------------------------------------------------------------------------------------------------------------------------------------
--③光コラボ関連以外のコースオプション抽出ここまで========================================================================================================================
)
/* -----------------------------------------------------------------------------
 * 変更履歴
 * 20190213 今村 新規作成
 */ -----------------------------------------------------------------------------