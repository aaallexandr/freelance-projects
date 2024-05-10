WITH orders_data AS
   -- Создаем таблицу с заказами для расчета конверсий
  (SELECT cas.atom_id, 
          cdr.id_zakaza,
          device_type_id,
          data_sozdanija_zakaza::DATE,
          -- На уровне заказа нумеруем события по убыванию относительно даты оформления заказа
          -- Последнему по времени событию перед оформлением заказа будет присвоен номер = 1
          ROW_NUMBER () OVER (PARTITION BY cdr.id_zakaza
                              ORDER BY cas.datetime DESC) AS event_number_within_order
   FROM crm_data_raw AS cdr
   -- Присоединение таблиц с данными из веб-аналитики, справочника РК и расшифровкой устройства
   FULL JOIN core_analytics_seccions AS cas ON cdr.google_client_id = cas.client_id
   FULL JOIN pivotparts_atoms_with_extensions AS pawe ON pawe.atom_id = cas.atom_id
   FULL JOIN core_device_types AS cdt ON cdt.id = cas.device_type_id
   WHERE cas.datetime <= cdr.data_sozdanija_zakaza -- Отбрасываем все даты событий, которые идут после оформления заказа
     AND cas.atom_id IN -- Оставляем только те Atom ID, которые есть в данных из рекламных кабинетов
       (SELECT DISTINCT atom_id
        FROM pivotparts_source_data)
     AND cas.atom_id NOT IN -- И исключем Atom ID, которые ассоциированы с блогерами
       (SELECT DISTINCT atom_id
        FROM pivotparts_atoms_with_extensions
        WHERE campaign LIKE '%Blogger%'
          OR SOURCE LIKE '%Блогер%')
     AND kupony IS NULL -- Дополнительно исключаем заказы, атрибцированные блогерами
     AND zakaz_oplachen = TRUE) -- Оставляем только оплаченные заказы для расчета конверсий
-- Начало основного запроса, выведение полей
SELECT DATE, psd.atom_id,
             CASE WHEN impressions IS NULL THEN 0 ELSE impressions END,
             CASE WHEN spend IS NULL THEN 0 ELSE spend END,
             CASE WHEN clicks IS NULL THEN 0 ELSE clicks END,
             NAME AS device_name,
             description AS device_description,
             campaign,
             source,
             source_group,
             groups_of_source_group,
             extension_brand,
             platniy AS paid_source,
             CASE WHEN sessions IS NULL THEN 0 ELSE sessions END,
             CASE WHEN events IS NULL THEN 0 ELSE events END,
             CASE WHEN orders IS NULL THEN 0 ELSE orders END
FROM pivotparts_source_data AS psd
-- Присоединение данных с расшифровкой устройств и справочника рекламных кампаний
LEFT JOIN core_device_types AS cdt ON cdt.id = psd.device_type_id
LEFT JOIN pivotparts_atoms_with_extensions AS pawe ON pawe.atom_id = psd.atom_id
-- Присоединяем сгруппированную таблицу с данными из веб-аналитики
LEFT JOIN
  (SELECT datetime::DATE,
          atom_id,
          device_type_id,
          SUM (CASE
                   WHEN sessions = TRUE THEN 1
                   ELSE 0
               END) AS sessions, -- В отдельном поле считаем число сессий
              COUNT (sessions) AS events -- Считаем число событий, включая сессии
   FROM core_analytics_seccions AS cas
   GROUP BY datetime::DATE, -- Группируем по дате, Atom ID и устройству
            atom_id,
            device_type_id) AS cas ON cas.atom_id = psd.atom_id AND cas.datetime = psd.DATE AND cas.device_type_id = psd.device_type_id
-- Присоединяем таблицу с заказами из подзапроса          
LEFT JOIN
  (SELECT data_sozdanija_zakaza,
          device_type_id,
          atom_id,
          COUNT (id_zakaza) AS orders -- Считаем число заказов
   FROM orders_data
   WHERE event_number_within_order = 1 -- Оставяем только последнее событие перед оформлением заказа
   GROUP BY data_sozdanija_zakaza,
            atom_id,
            device_type_id) AS od ON od.atom_id = psd.atom_id -- Присоединение по пяти параметрам
AND od.data_sozdanija_zakaza = psd.DATE
AND od.device_type_id = cas.device_type_id
AND cas.datetime = psd.DATE
AND cas.device_type_id = psd.device_type_id
WHERE psd.atom_id NOT IN
    (SELECT DISTINCT atom_id -- Убираем блогерские ID из финальной таблицы
FROM pivotparts_atoms_with_extensions
     WHERE campaign LIKE '%Blogger%'
       OR SOURCE LIKE '%Блогер%')
ORDER BY psd.DATE,
         psd.atom_id,
         psd.device_type_id
