-- Создаем основную таблицу через подзапрос
WITH main_table AS
  (SELECT cdr.id_zakaza AS order_id,
          -- Заменяем Atom ID на ноль для непривязанных заказов
          CASE
              WHEN cas.atom_id IS NULL THEN 0
              ELSE cas.atom_id
          END,
          cdr.id_polzovatelja AS user_id,
          cdr.status_zakaza AS order_status,
          cdr.data_sozdanija_zakaza AS order_creation_date,
          cdr.data_oplaty AS payment_date,
          -- В ходе присоединения данные из таблиц будут дублироваться
          -- Для того, чтобы в будущем учесть дубликаты, их нужно пометить с помощью оконной функции
          CASE
              WHEN ROW_NUMBER() OVER (PARTITION BY id_zakaza) > 1 THEN 'Дубликат'
              ELSE 'Не дубликат'
          END AS order_duplicate,
          -- Заменяем оплаченную сумму на ноль там, где значение суммы = NULL
          CASE
              WHEN cdr.oplachennaja_summa IS NULL THEN 0
              ELSE cdr.oplachennaja_summa
          END AS paid_sum,
          cdr.data_otmeny_zakaza AS cancellation_date,
          cdr.nazvanie_sluzhby_dostavki AS delivery_name,
          cdr.nepodarki_tovarov_sht AS commercial_goods,
          cdr.podarki_tovarov_sht AS present_goods,
          cdr.gorod AS address,
          cdr.kupony,
          cas.client_id AS google_client_id,
          cas.datetime AS event_datetime,
          -- Нумерация событий внутри каждого заказа, последнее событие перед заказом будет под номером 1
          -- Это понадобится для выделение источника атрибуции
          ROW_NUMBER () OVER (PARTITION BY cdr.id_zakaza
                              ORDER BY cas.datetime DESC) AS event_number_within_order, 
          -- Нумерация заказов для каждого пользователя в порядке возрастания даты создания заказа  
          -- Эти данные понадобятся для расчета конверсии в повторную покупку                   
          DENSE_RANK () OVER (PARTITION BY cdr.id_polzovatelja
                     ORDER BY cdr.data_sozdanija_zakaza) AS order_number_within_user, 
          cas.sessions,
          pawe.campaign,
          pawe.source,
          pawe.source_group,
          pawe.groups_of_source_group,
          pawe.extension_brand,
          pawe.blogger,
          pawe.platniy,
          cept.elly_payment_type,
          cdt.NAME AS device_name,
          cdt.description AS device_description,
          -- Выделяем три статуса заказа
          CASE
              WHEN zakaz_oplachen = TRUE THEN 'Оплачен'
              WHEN zakaz_otmenen = TRUE THEN 'Отменен'
              ELSE 'Новый'
          END AS order_status,
          -- На уровне даты создания заказа и Atom ID выделяем дубликаты
          CASE
              WHEN ROW_NUMBER() OVER (PARTITION BY cas.atom_id,
                                          data_sozdanija_zakaza::DATE ORDER BY cas.datetime DESC) > 1 THEN 'Дубликат'
             ELSE 'Не дубликат'
             END AS atom_duplicate,
         -- Заменяем NULL на нули
         CASE
             WHEN psd.impressions IS NULL THEN 0
         ELSE psd.impressions
         END,
         CASE
             WHEN psd.clicks IS NULL THEN 0
             ELSE psd.clicks
         END,
         CASE
             WHEN psd.spend IS NULL THEN 0
             ELSE psd.spend
         END,
         -- Выделение платной и бесплатной доставки
         CASE 
             WHEN cdr.stoimost_dostavki IS NOT NULL
                  OR cdr.stoimost_dostavki !=0 THEN TRUE
             ELSE FALSE
         END AS paid_delivery,
         -- Выделение тестовых заказов
         CASE 
             WHEN cdr.id_polzovatelja IN
                    (SELECT DISTINCT id_polzovatelja
                     FROM client_binding_rules_crm_test_userid)
                  OR (cdr.nepodarki_summa - cdr.nepodarki_skidka) < 10 THEN TRUE
             ELSE FALSE
         END AS test,
         -- Выделение трех типов источников
         CASE 
             WHEN cas.atom_id IN
                    (SELECT DISTINCT atom_id
                     FROM pivotparts_atoms_with_extensions
                     WHERE campaign LIKE '%Blogger%'
                       OR SOURCE LIKE '%Блогер%' ) THEN 'Блогеры'
             WHEN cas.atom_id IN
                    (SELECT DISTINCT atom_id
                     FROM pivotparts_source_data
                     WHERE atom_id NOT IN
                         (SELECT DISTINCT atom_id
                          FROM pivotparts_atoms_with_extensions
                          WHERE campaign LIKE '%Blogger%'
                            OR SOURCE LIKE '%Блогер%' ) ) THEN 'Прямая реклама'
             WHEN cas.atom_id IN
                    (SELECT atom_id
                     FROM pivotparts_atoms_with_extensions
                     WHERE atom_id NOT IN
                         (SELECT DISTINCT atom_id
                          FROM pivotparts_atoms_with_extensions
                          WHERE campaign LIKE '%Blogger%'
                            OR SOURCE LIKE '%Блогер%' )
                       AND atom_id NOT IN
                         (SELECT DISTINCT atom_id
                          FROM pivotparts_source_data
                          WHERE atom_id NOT IN
                              (SELECT DISTINCT atom_id
                               FROM pivotparts_atoms_with_extensions
                               WHERE campaign LIKE '%Blogger%'
                                 OR SOURCE LIKE '%Блогер%'))) THEN 'Партнеры и рассылки'
            ELSE 'Неизвестно'
        END AS source_type
   -- Присоедияем все таблицы в исходном виде, кроме данных из рекламных кабинетов
   FROM crm_data_raw AS cdr
   LEFT JOIN core_analytics_seccions AS cas ON cdr.google_client_id = cas.client_id
   LEFT JOIN pivotparts_atoms_with_extensions AS pawe ON pawe.atom_id = cas.atom_id
   LEFT JOIN crm_elly_payment_type AS cept ON cept.id = cdr.elly_payment_type_id
   LEFT JOIN core_device_types AS cdt ON cdt.id = cas.device_type_id
   -- Для присоединения данных из рекламных кабинетов преобразуем таблицу
   LEFT JOIN
     (SELECT atom_id,
             DATE,
             SUM(impressions) AS impressions,
             SUM(clicks) AS clicks,
             SUM(spend) AS spend
      FROM pivotparts_source_data
      GROUP BY atom_id, DATE -- Сгруппируем данные
      ORDER BY atom_id DESC, DATE) AS psd ON psd.atom_id = cas.atom_id
                                   AND psd.DATE = cdr.data_sozdanija_zakaza::DATE),
     -- Создадим вторую таблицу в подзапросе для расчета цикла покупки
     first_event_table AS
  (SELECT order_id,
          MIN(event_datetime) AS first_event -- Внутри каждого заказа выделим первую дату события
   FROM main_table
   GROUP BY order_id)
   -- Выгружаем основную таблицу
   SELECT *,
          mt.order_creation_date - first_event AS purchase_cycle, -- Считаем цикл покупки 
          -- Учитывыем промокод для атрибуции блогеров
          CASE
              WHEN kupony IS NOT NULL THEN 'Блогеры'
              ELSE source_type
          END AS attrubution_source
    FROM main_table AS mt
    LEFT JOIN first_event_table AS fe ON mt.order_id = fe.order_id -- Присоединяем таблицу с первыми событиями 
    ORDER BY mt.order_id,
         event_datetime,
         event_number_within_order DESC;
