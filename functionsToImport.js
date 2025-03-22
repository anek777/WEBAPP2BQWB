const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
const ssid = spreadsheet.getId();

function getProjectID_() {
  return (projectId = "fresh-gravity-322709");
}




function uploadDiagnostic_(data) {

  const projectId = getProjectID_();
  const datasetId = "clients";
  const tableId = "wb_clients_full"

  Logger.log(
    "Start: " +
      getProjectID_() +
      "client: " +
      datasetId +
      " table: " +
      tableId
  );

  var request = {
    configuration: {
      load: {
        destinationTable: {
          projectId: projectId,
          datasetId: datasetId,
          tableId: tableId
        },
        schema: {
          fields: [
            {name: 'sid', type: 'STRING'},
            {name: 'expFormatted', type: 'TIMESTAMP'},
            {name: 'categories', type: 'STRING'},
            {name: 'dataset', type: 'STRING'},
            {name: 'ss_id', type: 'STRING'},
            {name: 'ss_name', type: 'STRING'},
            {name: 'scriptId', type: 'STRING'},
            {name: 'ownerEmail', type: 'STRING'},
            {name: 'created_date', type: 'TIMESTAMP'},
            {name: 'viewers_list', type: 'STRING'},
            {name: 'editors_list', type: 'STRING'},
            {name: 'everyone_can_edit', type: 'STRING'},
            {name: 'executorEmail', type: 'STRING'},
            {name: 'cr_date', type: 'TIMESTAMP'}
          ]
        },
        writeDisposition: 'WRITE_APPEND',
        sourceFormat: 'NEWLINE_DELIMITED_JSON', // используем формат JSON
      }
    }
  };

  var dataToLoad = [
    {
      sid: data.sid,
      expFormatted: data.expFormatted,
      categories: data.categories,
      dataset: data.dataset,
      ss_id: data.ss_id,
      ss_name: data.ss_name,
      scriptId: data.scriptId,
      ownerEmail: data.ownerEmail,
      created_date: data.created_date,
      viewers_list: data.viewers_list,
      editors_list: data.editors_list,
      everyone_can_edit: data.everyone_can_edit,
      executorEmail: data.executorEmail,
      cr_date: data.cr_date
    }
  ];

  console.log(dataToLoad);

  var dataBlob = Utilities.newBlob(JSON.stringify(dataToLoad[0]), 'application/json');
  // console.log(dataBlob);

  var job = BigQuery.Jobs.insert(request, projectId, dataBlob);

  // Проверяем на ошибки
  if (job.status.errors && job.status.errors.length > 0) {
    Logger.log('Error inserting data: ' + JSON.stringify(job.status.errors));
  } else {
    Logger.log('Data successfully inserted into BigQuery');
  }

}

function uploadData(range, targetClient, targetTable, clearNull) {
  if (clearNull == null) {
    var clearNull = false;
  }

  const projectId = getProjectID_();

  var values = range.getValues();

  if (clearNull) {
    Logger.log("Start clearing null rows: " + values.length + " total rows");
    var dirtyArray = values;
    // Initiate an empty array to fill it with the non empty data from your Array
    var cleanArr = [];
    // Check every array inside your main array
    dirtyArray.forEach(function (el) {
      // If there are null values, this will clean them
      var filteredArr = el.filter(function (e) {
        return e != "";
      });
      // If the array ends up empty, don't push it into your clean array
      if (filteredArr.length) cleanArr.push(filteredArr);
    });

    values = cleanArr;
    Logger.log(
      "Cleared: " +
        (dirtyArray.length - values.length) +
        " null rows. Total now: " +
        values.length
    );
  }

  Logger.log(
    "Start: " +
      getProjectID_() +
      "client: " +
      targetClient +
      " table: " +
      targetTable
  );
  Logger.log("Values " + values.length);

  var values = range.getValues();
  var rowsCSV = values.join("\n");
  var data = Utilities.newBlob(rowsCSV, "application/octet-stream");

  Logger.log("Blob done.");

  // Create the data upload job.
  const job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: projectId,
          datasetId: targetClient,
          tableId: targetTable,
        },
        skipLeadingRows: 0,
        writeDisposition: "WRITE_TRUNCATE",
      },
      labels: {
        "bq" : "uploaddata"
      },

    },
  };
  try {
    BigQuery.Jobs.insert(job, projectId, data);
    Logger.log(
      "Load job started. Check on the status of it here: " +
        "https://bigquery.cloud.google.com/jobs/%s",
      projectId
    );
  } catch (err) {
    Logger.log("unable to insert job: " + err);
  }
}

/**
 * Импортирует данные всех заказов из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса.
 * @param {string} [params.version="basic"] - Версия запроса.
 * @param {string} [params.sheet_name="Список заказов"] - Название листа, на котором следует разместить данные.
 * @param {number} [params.start_col=2] - Начальная колонка для размещения данных.
 * @param {number} [params.start_row=5] - Начальная строка для размещения данных.
 * @param {boolean} [params.need_headers=false] - Наличие заголовков в результирующей таблице.
 * @param {number} [params.num_cols=13] - Количество колонок данных.
 * @param {string} [params.start_date=null] - Начальная дата для фильтрации заказов.
 * @param {string} [params.with_cost_price=false] - вывод с\с из БД на лист Список заказов для версии по дням
 */
function importWBFUllOrders(
  dataset_name,
  params = {
    version: "basic",
    sheet_name: "Список заказов",
    start_col: 2,
    start_row: 5,
    need_headers: false,
    num_cols: 13,
    start_date: null,
    with_cost_price: false
  }
) {
  let query_text = "";
  let start_date_query_part = "";

  if (params.version === "basic") {
    if (
      typeof params.start_date !== "undefined" &&
      params.start_date !== null
    ) {
      start_date_query_part = 'and order_dt >="' + params.start_date + '" ';
    }

    query_text =
      "select final_srid, barcode, gi_id, order_dt, sale_dt, REPLACE(cast(orderPrice as string), '.', ',') as orderPrice,  REPLACE(cast(payedPrice as string), '.', ',') as payedPrice, REPLACE(cast(FORMAT('%.2f%%', ROUND(commission_percent * 100, 2)) as string), '.', ',')  as commission_percent, REPLACE(cast(Comission as string), '.', ',') as Comission, REPLACE(cast(deliveryCost as string), '.', ',') as deliveryCost, selfsalestatus, status, region from `fresh-gravity-322709." +
      dataset_name +
      ".orderListWithSelfSales_final` where barcode is not null " +
      start_date_query_part +
      "order by order_dt desc";
  } else if (params.version === "daily") {
      if (
        typeof params.start_date !== "undefined" &&
        params.start_date !== null
      ) {
        start_date_query_part = 'and sale_dt >="' + params.start_date + '" ';
      }

      if (
        typeof params.with_cost_price !== "undefined" &&
        params.with_cost_price === true
      ) {
        query_text =
          "select barcode, sale_dt, REPLACE(cast(shipment_days as string), '.', ',') as shipment_days, orders, sales, REPLACE(cast(orderPrice as string), '.', ',') as orderPrice,  REPLACE(cast(payedPrice as string), '.', ',') as payedPrice, REPLACE(cast(FORMAT('%.2f%%', ROUND(commission_percent * 100, 2)) as string), '.', ',')  as commission_percent, REPLACE(cast(Comission as string), '.', ',') as Comission,  REPLACE(cast(deliveryCost as string), '.', ',') as deliveryCost, REPLACE(cast(futureDeliveryCosts as string), '.', ',') as futureDeliveryCosts, selfsalestatus, REPLACE(cast(cost_price as string), '.', ',') as cost_price from `fresh-gravity-322709." +
          dataset_name +
          ".final_orderList_by_days` where barcode is not null " +
          start_date_query_part +
          "order by sale_dt desc";
        }

      else{
          query_text =
          "select barcode, sale_dt, REPLACE(cast(shipment_days as string), '.', ',') as shipment_days, orders, sales, REPLACE(cast(orderPrice as string), '.', ',') as orderPrice,  REPLACE(cast(payedPrice as string), '.', ',') as payedPrice, REPLACE(cast(FORMAT('%.2f%%', ROUND(commission_percent * 100, 2)) as string), '.', ',')  as commission_percent, REPLACE(cast(Comission as string), '.', ',') as Comission,  REPLACE(cast(deliveryCost as string), '.', ',') as deliveryCost, REPLACE(cast(futureDeliveryCosts as string), '.', ',') as futureDeliveryCosts, selfsalestatus from `fresh-gravity-322709." +
          dataset_name +
          ".final_orderList_by_days` where barcode is not null " +
          start_date_query_part +
          "order by sale_dt desc";
        }
    }   else {
    console.log("Version: " + params.version + "I cant make it, sorry");
    return;
  }

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols,
    15000,
    true,
    false,
    params.with_cost_price
  );
}


function updateSettings(){
  // обновление списка bonus_type_name на листе Настройки
  Logger.log("Updating bonus_type_name list");
  importWBbonusTypeName();

  // обновление списка Баскетов на листе Настройки
  Logger.log("Updating baskets list");
  importWBbaskets();

  // обновление списка Включить в Сверку (Продажи) на листе Настройки
  Logger.log("Updating include_in_sales list");
  importWBincludeInSales();

}


/**
 * Импортирует список ссылок для картинок на лист Настройки из таблицы БД wb_baskets
 * @param {Object} params - Параметры запроса, включая название листа.
 */
function importWBbaskets(
  params = {
        need_headers: false
        }
) {
  const queryText =
    "SELECT nm_id, link FROM  `fresh-gravity-322709.system.wb_baskets`";
  return runQuery(queryText, params.needHeaders);
}


/**
 * Импортирует список supplier_oper_name на лист Настройки из таблицы БД wb_include_in_sales
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа.
 * @param {string} params.sheet_name - Название листа, на котором следует разместить данные.
 */
function importWBincludeInSales(
  params = {
        sheet_name: "Настройки",
        start_col: 38,
        start_row: 6,
        need_headers: false,
        delete_rows: false,
        delete_cols: false
        }
) {
  const queryText =
    "SELECT supplier_oper_name FROM  `fresh-gravity-322709.system.wb_include_in_sales`";

  runQuery(queryText, params.sheet_name, params.start_col, params.start_row, params.need_headers, undefined, undefined, params.delete_rows, params.delete_cols);
}


/**
 * Импортирует данные отчета по неделе из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа.
 * @param {string} params.sheet_name - Название листа, на котором следует разместить данные.
 */
function importWBWeekReportData(dataset_name) {

  // проверка на версию отчетности, если по дням, загружаем данные для зеленого блока Сверки
  try {
    Logger.log("Get sheet by name");

    var refusalSheetName = "Отказы";

    if (getSheetId_(ssid, refusalSheetName) !== undefined) {
      Logger.log(
        "Daily version, run the function importWBOrdersWithoutDopsWeekly"
      );
      importWBOrdersWithoutDopsWeekly(dataset_name);
    }
  } catch (err) {
    Logger.log(`[ERROR]: ${err.message}\n${err.stack}`);
    throw new Error(err.message);
  }

  // загружаем данные для голубого блока Сверки
  params = {
    sheet_name: "О файле из БД",
    start_col: 1,
    start_row: 2,
  };
  const queryText =
    "SELECT realizationreport_id, retail_amount, ppvz_for_pay, delivery_rub FROM" +
    "(SELECT realizationreport_id, REPLACE(cast(sum(retail_amount) as string), '.', ',') as retail_amount, REPLACE(cast(sum(ppvz_for_pay) as string), '.', ',')  as ppvz_for_pay, REPLACE(cast( sum(delivery_rub) as string), '.', ',') as delivery_rub FROM  `fresh-gravity-322709." +
    dataset_name +
    ".reportDetailByPeriodFinalSrid`  group by realizationreport_id) order by realizationreport_id desc";

  runQuery(queryText, params.sheet_name, params.start_col, params.start_row);
}

/**
 * Импортирует данные продуктов из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа.
 * @param {string} [params.sheet_name="ШК из Реализаций"] - Название листа, на котором следует разместить данные.
 */
function importWBProducts(
  dataset_name,
  params = { sheet_name: "ШК из Реализаций" }
) {
  const query_text =
    "SELECT distinct barcode, brand_name, sa_name, subject_name, ts_name, nm_id FROM " +
    "(SELECT distinct barcode, brand_name, sa_name, subject_name, ts_name, nm_id FROM `fresh-gravity-322709." +
    dataset_name +
    ".reportDetailByPeriodFinalSrid` " +
    "UNION ALL " +
    "SELECT distinct barcode, brand as brand_name, supplierArticle as sa_name, subject as subject_name, techSize as ts_name, nmId as nm_id FROM `fresh-gravity-322709." +
    dataset_name +
    ".sales` " +
    " UNION ALL " +
    "SELECT distinct barcode, brand as brand_name, supplierArticle as sa_name, subject as subject_name, techSize as ts_name, nmId as nm_id FROM `fresh-gravity-322709." +
    dataset_name +
    ".orders` )";

  runQuery(query_text, params.sheet_name);

  // Устанавливаем текстовый формат для размеров
  var sheets_prop_goods = getSheetId_(ssid, 'Товары');
  var sheetID_goods = sheets_prop_goods[0];
  var sheet_goods = spreadsheet.getSheetById(sheetID_goods);
  sheet_goods.getRange("AN1:AN").setNumberFormat('@STRING@');

  var sheets_prop = getSheetId_(ssid, params.sheet_name);
  var sheetID = sheets_prop[0];
  var sheet = spreadsheet.getSheetById(sheetID);
  sheet.getRange("E1:E").setNumberFormat('@STRING@');
}

/**
 * Импортирует данные о стоимости хранения из заданного dataset в Google BigQuery и записывает их в Google Sheets.
 *
 * @param {string} dataset_name - Название dataset в Google BigQuery.
 * @param {Object} [params] - Параметры для настройки записи данных в Google Sheets.
 * @param {string} [params.sheet_name="Хранение"] - Имя листа в Google Sheets.
 * @param {number} [params.start_col=1] - Начальный столбец для записи данных.
 * @param {number} [params.start_row=1] - Начальная строка для записи данных.
 * @param {boolean} [params.need_headers=false] - Нужно ли записывать заголовки столбцов.
 * @param {number} [params.num_cols=4] - Число столбцов для записи данных.
 *
 * @returns {void}
 */
function importStorageFee(
  dataset_name,
  params = {
    sheet_name: "Хранение",
    start_col: 1,
    start_row: 1,
    need_headers: false,
    num_cols: 4,
  }
) {
  const query_text =
    "select date_from, date_to, nm_id, sum(storage_fee) as sum from (select date(date_from) as date_from, date(date_to) as date_to, nm_id, cast(storage_fee as int64) as storage_fee  from `fresh-gravity-322709." +
    dataset_name +
    ".reportDetailByPeriod_v3`) group by date_from, date_to, nm_id order by date_from";

  const sheetName = "Хранение";

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}

/**
 * Импортирует данные об удержаниях и компенсациях из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, начальные координаты, наличие заголовков и количество колонок.
 * @param {string} [params.sheet_name="Удержания и компенсации"] - Название листа, на котором следует разместить данные.
 * @param {number} [params.start_col=1] - Начальная колонка для размещения данных.
 * @param {number} [params.start_row=7] - Начальная строка для размещения данных.
 * @param {boolean} [params.need_headers=false] - Наличие заголовков в результирующей таблице.
 * @param {number} [params.num_cols=8] - Количество колонок данных.
 * @param {string} [params.start_date=null] - Начальная дата для фильтрации данных.
 */
function importCompAndPenalty(
  dataset_name,
  params = {
    sheet_name: "Удержания и компенсации",
    start_col: 1,
    start_row: 7,
    need_headers: false,
    num_cols: 8,
    start_date: null,
  }
) {
  let start_date_query_part = "";
  if (typeof params.start_date !== "undefined" && params.start_date !== null) {
    start_date_query_part = 'where date >= "' + params.start_date + '" ';
  }
  const query_text =
    "select brand_name,  barcode, DATE(date), REPLACE(cast(compensation as string), '.', ',') as compensation, REPLACE(cast(penalty as string), '.', ',') as penalty, supplier_oper_name, doc_type_name, REPLACE(cast(cost_price as string), '.', ',') as cost_price from `fresh-gravity-322709." +
    dataset_name +
    ".compensation_and_penalty` " +
    start_date_query_part +
    "order by date desc";

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}

/**
 * Импортирует данные о возвратах из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, начальные координаты, наличие заголовков и количество колонок.
 * @param {string} [params.sheet_name="Возвраты"] - Название листа, на котором следует разместить данные.
 * @param {number} [params.start_col=2] - Начальная колонка для размещения данных.
 * @param {number} [params.start_row=1] - Начальная строка для размещения данных.
 * @param {boolean} [params.need_headers=false] - Наличие заголовков в результирующей таблице.
 * @param {number} [params.num_cols=9] - Количество колонок данных.
 * @param {string} [params.start_date=null] - Начальная дата для фильтрации данных.
 */
function importWBReturns(
  dataset_name,
  params = {
    sheet_name: "Возвраты",
    start_col: 2,
    start_row: 1,
    need_headers: true,
    num_cols: 9,
    start_date: null,
  }
) {
  let start_date_query_part = "";
  if (typeof params.start_date !== "undefined" && params.start_date !== null) {
    start_date_query_part = 'and order_dt >= "' + params.start_date + '" ';
  }
  const query_text =
    "select srid, barcode, gi_id, order_dt, sale_dt as return_dt, REPLACE(cast(orderPrice as string), '.', ',') as orderPrice,  REPLACE(cast(payedPrice as string), '.', ',') as payedPrice, REPLACE(cast(deliveryCost as string), '.', ',') as deliveryCost, region  from `fresh-gravity-322709." +
    dataset_name +
    ".returns_final` where barcode is not null " +
    start_date_query_part +
    "order by sale_dt desc";

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}

/**
 * Импортирует данные об отказах из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, начальные координаты, наличие заголовков и количество колонок.
 * @param {string} [params.sheet_name="Отказы"] - Название листа, на котором следует разместить данные.
 * @param {number} [params.start_col=2] - Начальная колонка для размещения данных.
 * @param {number} [params.start_row=1] - Начальная строка для размещения данных.
 * @param {boolean} [params.need_headers=false] - Наличие заголовков в результирующей таблице.
 * @param {number} [params.num_cols=9] - Количество колонок данных.
 * @param {string} [params.start_date=null] - Начальная дата для фильтрации данных.
 */
function importWBRefusals(
  dataset_name,
  params = {
    sheet_name: "Отказы",
    start_col: 2,
    start_row: 1,
    need_headers: true,
    num_cols: 9,
    start_date: null,
  }
) {
  let start_date_query_part = "";
  if (typeof params.start_date !== "undefined" && params.start_date !== null) {
    start_date_query_part = 'and order_dt >= "' + params.start_date + '" ';
  }
  const query_text =
    "select srid, barcode, gi_id, order_dt, sale_dt as refusal_dt, REPLACE(cast(orderPrice as string), '.', ',') as orderPrice,  REPLACE(cast(payedPrice as string), '.', ',') as payedPrice, REPLACE(cast(deliveryCost as string), '.', ',') as deliveryCost, region  from `fresh-gravity-322709." +
    dataset_name +
    ".refusals_final` where barcode is not null " +
    start_date_query_part +
    "order by sale_dt desc";

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}

/**
 * Импортирует данные о заказах за последние 30 дней из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, начальные координаты, наличие заголовков и количество колонок.
 * @param {string} [params.sheet_name="заказы 30 дн"] - Название листа, на котором следует разместить данные.
 * @param {number} [params.start_col=1] - Начальная колонка для размещения данных.
 * @param {number} [params.start_row=1] - Начальная строка для размещения данных.
 * @param {boolean} [params.need_headers=false] - Наличие заголовков в результирующей таблице.
 * @param {number} [params.num_cols=3] - Количество колонок данных.
 */
function importWBOrders30days(
  dataset_name,
  params = {
    sheet_name: "заказы 30 дн",
    start_col: 1,
    start_row: 1,
    need_headers: true,
    num_cols: 3,
  }
) {
  const query_text =
    "select date, hour, barcode from `fresh-gravity-322709." +
    dataset_name +
    ".orders_30_days` where barcode is not null order by date desc";

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}

/**
* Импортирует данные о продажах за последние 30 дней из указанного набора данных.
* @param {string} dataset_name - Название набора данных для выполнения запроса.
* @param {Object} params - Параметры запроса, включая название листа, начальные координаты, наличие заголовков и количество колонок.
* @param {string} [params.sheet_name="продажи 30 дн"] - Название листа, на котором следует разместить данные.
* @param {number} [params.start_col=1] - Начальная колонка для размещения данных.
* @param {number} [params.start_row=1] - Начальная строка для размещения данных.
* @param {boolean} [params.need_headers=false] - Наличие заголовков в результирующей таблице.
* @param {number} [params.num_cols=3] - Количество колонок данных.
*/
function importWBSales30days(
  dataset_name,
  params = {
    sheet_name: "продажи 30 дн",
    start_col: 1,
    start_row: 1,
    need_headers: true,
    num_cols: 3,
  }
) {
  const query_text =
    "select date, hour, barcode from `fresh-gravity-322709." +
    dataset_name +
    ".sales_30_days` where barcode is not null order by date desc";

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}

/**
 * Импортирует данные о перемещениях продуктов из указанного набора данных с условием, что количество перемещений больше указанного значения.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, начальные координаты и минимальное количество перемещений.
 * @param {string} [params.sheet_name="Брак на ВБ"] - Название листа, на котором следует разместить данные.
 * @param {number} [params.start_col=4] - Начальная колонка для размещения данных.
 * @param {number} [params.start_row=6] - Начальная строка для размещения данных.
 * @param {number} [params.movements_more_than=3] - Минимальное количество перемещений, которое должно быть удовлетворено.
 * @param {string} [params.start_date=null] - Начальная дата для фильтрации данных.
 */
function importWBmovements(
  dataset_name,
  params = {
    sheet_name: "Брак на ВБ",
    start_col: 4,
    start_row: 6,
    need_headers: false,
    movements_more_than: 3,
    start_date: null,
  }
) {
  let start_date_query_part = "";
  if (typeof params.start_date !== "undefined" && params.start_date !== null) {
    start_date_query_part = 'and last_order >= "' + params.start_date + '" ';
  }

  query_text =
    "select shk_id, barcode, movements, REPLACE(cast(delivery as string), '.', ',') as delivery, first_order, last_order from `fresh-gravity-322709." +
    dataset_name +
    ".movements_final` where movements > " +
    params.movements_more_than +
    " " +
    start_date_query_part +
    "order by movements desc";

  runQuery(query_text, params.sheet_name, params.start_col, params.start_row, params.need_headers);
}

/**
 * Импортирует историю остатков товаров из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, начальные координаты, наличие заголовков и количество колонок.
 * @param {string} [params.sheet_name="Остатки товаров исх."] - Название листа, на котором следует разместить данные.
 * @param {number} [params.start_col=1] - Начальная колонка для размещения данных.
 * @param {number} [params.start_row=1] - Начальная строка для размещения данных.
 * @param {boolean} [params.need_headers=false] - Наличие заголовков в результирующей таблице.
 * @param {number} [params.num_cols=3] - Количество колонок данных.
 * @param {string} [params.group=null] - Группировка значений, пример "month".
 */
function importWBStocksHistory(
  dataset_name,
  params = {
    sheet_name: "Остатки товаров исх.",
    start_col: 1,
    start_row: 1,
    need_headers: true,
    num_cols: 3,
    group: null,
    only_zero_stock: false,
    start_date: "2024-01-01"
  }
) {
  let filter_date = "";
  let quantity_condition = "quantity > 0 ";
  let start_date_condition = "";

  if (typeof params.group !== "undefined" && params.group == "month") {
    filter_date =
      "AND DATE(lastChangeDate) = DATE_SUB(DATE_ADD(DATE_TRUNC(DATE(lastChangeDate), MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY) OR (DATE(lastChangeDate) = CURRENT_DATE() AND CURRENT_DATE() != DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY)) ";
  }

  if (typeof params.only_zero_stock !== "undefined" && params.only_zero_stock == true) {
    quantity_condition = "quantity = 0 ";
  }

  if (typeof params.start_date !== "undefined") {
    console.log(params.start_date);
    start_date_condition = "and date(lastChangeDate) >= date('" + params.start_date + "') ";
  }


  const query_text =
    "select barcode, sum(quantity) as quantity, DATE(lastChangeDate) as date from `fresh-gravity-322709." +
    dataset_name +
    ".stocks` where " +
    quantity_condition  +
    filter_date +
    start_date_condition +
    "group by barcode, lastChangeDate order by lastChangeDate desc";

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}

/**
 * Импортирует данные из BigQuery таблицы в Google Sheet.
 *
 * @param {string} dataset_name - Название набора данных в BigQuery.
 * @param {Object} [params] - Параметры для функции.
 * @param {string} [params.bq_table_name="reportDetailByPeriodFinalSrid"] - Название таблицы в BigQuery.
 * @param {string} [params.sheet_name="WeekReport"] - Название листа в Google Sheet.
 * @param {number} [params.start_col=1] - Номер начальной колонки для импорта в Google Sheet.
 * @param {number} [params.start_row=1] - Номер начальной строки для импорта в Google Sheet.
 * @param {boolean} [params.need_headers=true] - Нужно ли учитывать заголовки при импорте.
 * @param {number} [params.num_cols=60] - Количество колонок для импорта.
 * @param {string} [params.where_condition='limit 1000 order by rrd_id desc'] - Условие "WHERE" для запроса к BigQuery.
 */
function importTableSource(
  dataset_name,
  params = {
    bq_table_name: "reportDetailByPeriodFinalSrid",
    sheet_name: "WeekReport",
    start_col: 1,
    start_row: 1,
    need_headers: true,
    num_cols: 60,
    where_condition: "limit 1000 order by rrd_id desc"
  }
) {

  const query_text =
    "select * from `fresh-gravity-322709." +
    dataset_name +
    "." +
    params.bq_table_name +
    "` " + params.where_condition;

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}

/**
 * Импортирует данные рекламного объявления WB из набора данных и записывает их в таблицу.
 *
 * @param {string} dataset_name - Имя набора данных.
 * @param {Object} [params] - Параметры для импорта.
 * @param {string} [params.sheet_name="data_promo_wb"] - Имя страницы в таблице.
 * @param {number} [params.start_col=1] - Номер начальной колонки для записи данных.
 * @param {number} [params.start_row=1] - Номер начальной строки для записи данных.
 * @param {boolean} [params.need_headers=false] - Нужно ли включать заголовки.
 * @param {number} [params.num_cols=10] - Количество колонок для записи данных.
 * @param {string|null} [params.start_date=null] - Начальная дата для фильтрации данных в формате 'YYYY-MM-DD'.
 */
function importWBAdvertisment(
  dataset_name,
  params = {
    sheet_name: "data_promo_wb",
    start_col: 1,
    start_row: 1,
    need_headers: true,
    num_cols: 10
  }
) {

  var sheets_settings_prop = getSheetId_(ssid, 'Настройки');
  var sheets_settings_id = sheets_settings_prop[0];
  var sheet_settings = spreadsheet.getSheetById(sheets_settings_id);
  var settings_start_date = sheet_settings.getRange('D3').getValue();
  settings_start_date = Utilities.formatDate(settings_start_date, "GMT+3", 'yyyy-MM-dd');

  var date_today = Utilities.formatDate(new Date(), "GMT+3", 'yyyy-MM-dd');
  Logger.log('date_today = ' + date_today);

  let start_date_query_part = "";
    if (typeof settings_start_date !== "undefined" && settings_start_date !== null) {
      start_date_query_part = "and begin >= '" + settings_start_date + "' ";
    }

  const query_text =
    "select advertId, date(begin) as date, nmId, REPLACE(cast(views as string), '.', ',') as views, REPLACE(cast(clicks as string), '.', ',') as clicks, REPLACE(cast(basket as string), '.', ',') as basket, REPLACE(cast(sum as string), '.', ',') as sum, cast(orders as string) as orders, cast(status as string) as status, cast(type as string) as type from `fresh-gravity-322709." +
    dataset_name +
    ".advert` where begin < " + "'" + date_today + "'" + start_date_query_part + " order by date desc";

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}

/**
 * Выполняет загрузку данных о самовыкупах в BigQuery из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, диапазон ячеек.
 * @param {string} [params.sheet_name="Самовыкупы по скрипту"] - Название листа, с которого следует взять данные для загрузки.
 * @param {string} [params.range="B4:B"] - Диапазон ячеек с данными для загрузки.
 */
function uploadWBSelfsales(
  dataset_name,
  params = {
    sheet_name: "Самовыкупы по скрипту",
    range: "B4:B",
    debug: false,
    skipLeadingRows: 1,
  }
) {
  // Ввод данных BigQuery в качестве переменной.
  var projectId = getProjectID_();
  // Набор данных
  var datasetId = dataset_name;
  // Таблица
  var tableId = "selfsales";

  var writeDispositionSetting = "WRITE_TRUNCATE";

  // Название листа в Google Spreadsheet для экспорта в BigQuery:
  Logger.log(params.sheet_name);

  var sheets_prop = getSheetId_(ssid, params.sheet_name);
  var sheetID = sheets_prop[0];

  var file = SpreadsheetApp.getActiveSpreadsheet().getSheetById(
    sheetID
  );
  // Все данные
  var rows = file.getRange(params.range).getValues();
  var rowsCSV = rows.join("\n");
  var blob = Utilities.newBlob(rowsCSV, "text/csv");
  var data = blob.setContentType("application/octet-stream");
  if (params.debug) {
    Logger.log(rowsCSV);
  }

  // Создание задания на загрузку данных.
  var job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: projectId,
          datasetId: datasetId,
          tableId: tableId,
        },
        skipLeadingRows: params.skipLeadingRows,
        writeDisposition: writeDispositionSetting,
      },
      labels: {
        "bq" : "uploadwbselfsales"
      },
    },
  };
  if (params.debug) {
    Logger.log(job);
  }

  // Отправка задания в BigQuery для выполнения запроса.
  var runJob = BigQuery.Jobs.insert(job, projectId, data);
  var jobId = runJob.jobReference.jobId;
  Logger.log("jobId: " + jobId);
  Logger.log(runJob.status);
  Logger.log("FINISHED!");
}

/**
 * Импортирует данные о товарах для БД в BigQuery из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, диапазон ячеек и отладочный режим.
 * @param {string} [params.sheet_name="Товары"] - Название листа, с которого следует взять данные для загрузки.
 * @param {string} [params.range="C9:AU"] - Диапазон ячеек с данными для загрузки.
 * @param {boolean} [params.debug=false] - Флаг отладочного режима.
 * @param {number} [params.skipLeadingRows=0] - Количество пропускаемых начальных строк.
 * @param {number} [params.status_column=8] - Индекс колонки, в которой находится статус.
 * @param {number} [params.cogs_column=6] - Индекс колонки, в которой проверяется наличие себестоимости.
 * @param {number} [params.wb_art_column=5] - Индекс колонки, содержащей артикул WB.
 * @param {number} [params.size_column=4] - Индекс колонки, содержащей размер.
 * @param {number} [params.sales_column=44] - Индекс колонки, содержащей объем продаж.
 * @param {Array<string>} [params.statuses=["Выведен из продажи"]] - Массив статусов, исключаемых из данных.
 */
function uploadWBGoods(
  dataset_name,
  params = {
    sheet_name: "Товары",
    range: "C9:AU",
    debug: false,
    skipLeadingRows: 0,
    status_column: 8,
    cogs_column: 6,
    wb_art_column: 5,
    size_column: 4,
    sales_column: 44,
    statuses: ["Неизвестный ШК", "Дубль"]
  }
) {
  console.time("uploadWBGoods")
  var projectId = getProjectID_();
  var datasetId = dataset_name;
  var tableId = "goods";
  var writeDispositionSetting = "WRITE_TRUNCATE";

  if (params.debug) {
    Logger.log("Получаем лист");
  }

  var sheets_prop = getSheetId_(ssid, params.sheet_name);
  var sheetID = sheets_prop[0];
  var file = SpreadsheetApp.getActiveSpreadsheet().getSheetById(sheetID);

  if (params.debug) {
    Logger.log("Получаем значения");
  }

  var rows = file.getRange(params.range).getValues();

  if (params.debug) {
    Logger.log("rows.length " + rows.length);
  }

  // Фильтруем строки и убираем пустые строки
  if (params.debug) {
    Logger.log("Фильтруем строки");
  }

  var filteredRows = rows.filter(row => {
    var status = row[params.status_column];  // Колонка K
    var columnI = row[params.cogs_column];  // Колонка I
    return !params.statuses.includes(status) && columnI !== "" && row.some(cell => cell !== "");
  });

  if (params.debug) {
    Logger.log("filteredRows " + filteredRows.length);
  }

  if (filteredRows.length == 0) {
    Logger.log("filteredRows " + filteredRows.length + "\nЗагружать нечего. Заканчиваем работу.");
    return;
  }

  // Обрабатываем дубликаты
  if (params.debug) {
    Logger.log("Обрабатываем дубликаты");
  }

  var uniqueRowsMap = {};
  filteredRows.forEach(row => {
    var key = row[params.wb_art_column] + "-" + row[params.size_column]; // Объединяем колонки H и G в ключ
    var currentMaxValue = (uniqueRowsMap[key] && uniqueRowsMap[key][params.sales_column]) || -Infinity; // Колонка AU
    if (row[params.sales_column] > currentMaxValue) {
      uniqueRowsMap[key] = row;
    }
  });

  var uniqueRows = Object.values(uniqueRowsMap);
  if (params.debug) {
    Logger.log("uniqueRows " + uniqueRows.length);
  }

  const dubles = filteredRows.length - uniqueRows.length
  if (dubles > 0) {
    Logger.log("Отфильтровано " + dubles);
  }

  // Убираем пустые строки перед созданием CSV
  if (params.debug) {
    Logger.log("Вырезаем пустые строки");
  }

  var nonEmptyRows = uniqueRows.filter(row => row.join("").trim() !== "");

  if (params.debug) {
    Logger.log("nonEmptyRows " + nonEmptyRows.length);
  }

  // Обрезаем строки до первых семи колонок
  if (params.debug) {
    Logger.log("Обрезаем колонки");
  }

  var truncatedRows = nonEmptyRows.map(row => row.slice(0, 7));

  // Экранируем запятые
  truncatedRows = escapeCommas(truncatedRows);
  if (params.debug) {
    Logger.log("truncatedRows " + truncatedRows.length);
  }

  // Заменяем переносы строк пробелами
  const cleanedValues = truncatedRows.map(row =>
    row.map(cell => {
      if (typeof cell === "string") {
        return cell.replace(/[\n\r]+/g, " ");
      }
      return cell;
    })
  );

  var rowsCSV = cleanedValues.map(row => row.join(",")).join("\n");

  Logger.log(rowsCSV.split("\n").slice(0, 150).join("\n"));

  var blob = Utilities.newBlob(rowsCSV, "text/csv");
  var data = blob.setContentType("application/octet-stream");

  var job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: projectId,
          datasetId: datasetId,
          tableId: tableId,
        },
        skipLeadingRows: params.skipLeadingRows,
        writeDisposition: writeDispositionSetting,
      },
      labels: {
        "bq" : "uploadwbgoods"
      }
    },
  };

  if (params.debug) {
    Logger.log(job);
  }

try {
  var runJob = BigQuery.Jobs.insert(job, projectId, data);
  Logger.log("Job started: " + runJob.jobReference.jobId);
} catch (e) {
  Logger.log("Error: " + e.message);
  Logger.log("Generated CSV: \n" + rowsCSV);
}

  var jobId = runJob.jobReference.jobId;
  Logger.log("jobId: " + jobId);
  Logger.log(runJob.status);
  console.timeEnd("uploadWBGoods")
}


function importWBOrdersWeekly() {
  const queryText =
    "select barcode, sale_date, REPLACE(cast(shipment_days as string), '.', ',') as shipment_days, orders, sales, REPLACE(cast(orderPrices as string), '.', ',') as orderPrices, REPLACE(cast(orderPayedBezNDS as string), '.', ',') as orderPayedBezNDS, REPLACE(cast(orderPayed as string), '.', ',') as orderPayed, REPLACE(cast(commission_percent as string), '.', ',') as commission_percent, REPLACE(cast(comission_cost as string), '.', ',') as comission_cost, REPLACE(cast(deliveryCosts as string), '.', ',') as deliveryCosts, REPLACE(cast(futureDeliveryCosts as string), '.', ',') as futureDeliveryCosts, company from " +
    "(" +
    'SELECT *, "ALM" as company FROM `fresh-gravity-322709.client_2.order_list_final_weeks` where barcode is not null ' +
    "UNION ALL " +
    'SELECT *, "Pavloff" as company FROM `fresh-gravity-322709.client_3.order_list_final_weeks` where barcode is not null ' +
    "union all " +
    'SELECT *, "Prudnikov" as company FROM `fresh-gravity-322709.client_4.order_list_final_weeks` where barcode is not null ' +
    ") order by company asc, sale_date desc, barcode asc";

  const sheetName = "Продажи (позаказно)";

  runQuery(queryText, sheetName);
}

function importOzonOrdersDaily() {
  const queryText =
    "select item_sku, order_day, orders, sales, REPLACE(cast(orderPayed/1.2 as string), '.', ',') as orderPayedBezNDS, REPLACE(cast(orderPayed as string), '.', ',') as orderPayed, REPLACE(cast(-1*commission_percent as string), '.', ',') as commission_percent, REPLACE(cast(-1*comission_cost/1.2 as string), '.', ',') as comission_cost, REPLACE(cast(-1*deliveryCosts/1.2 as string), '.', ',') as delivery_costs, company from " +
    "( " +
    'SELECT *, "ALM" as company FROM `fresh-gravity-322709.client_43.order_list_final` ' +
    "union all " +
    'SELECT *, "Pavloff" as company FROM `fresh-gravity-322709.client_44.order_list_final` ' +
    "union all " +
    'SELECT *, "Prudnikov" as company FROM `fresh-gravity-322709.client_45.order_list_final` ' +
    ") order by company asc, order_day desc, item_sku asc";

  const sheetName = "Продажи (Озон)";

  runQuery(queryText, sheetName);
}

function runQuery(queryText, needHeaders = false, maxResultsPerPage = 15000, pageToken = null) {
    const projectId = getProjectID_();
    let queryResults, jobId, jobLocation;
  
    if (!pageToken) {
      // Новый запрос
      const initialQuery = startQuery_(queryText, maxResultsPerPage);
      queryResults = initialQuery.queryResults;
      jobId = initialQuery.jobId;
      jobLocation = initialQuery.jobLocation;
    } else {
      // Загрузка данных по токену
      queryResults = fetchNextPage_(projectId, jobId, jobLocation, pageToken, maxResultsPerPage);
    }
  
    const data = generateReport_(queryResults, needHeaders); 
    const nextPageToken = queryResults.pageToken || null;
  
    return {
      data,
      nextPageToken,
      jobId,
      jobLocation,
    };
  }

// Выполняем первичный запрос
function startQuery_(queryText, maxResultsPerPage) {
    const projectId = getProjectID_();
    const request = {
      query: queryText,
      labels: { bq: "runquery" },
      useLegacySql: false,
    };
  
    const queryResults = BigQuery.Jobs.query(request, projectId);
    let jobId = queryResults.jobReference.jobId;
    let jobLocation = queryResults.jobReference.location;
  
    // Проверяем статус выполнения
    let sleepTimeMs = 500;
    while (!queryResults.jobComplete) {
      Utilities.sleep(sleepTimeMs);
      sleepTimeMs *= 2;
      queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
        location: jobLocation,
        maxResults: maxResultsPerPage,
      });
    }
    
    return { queryResults, jobId, jobLocation };
  }
  
  // Функция получения данных по токену страницы
  function fetchNextPage_(projectId, jobId, jobLocation, pageToken, maxResultsPerPage) {
    const queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: pageToken,
      location: jobLocation,
      maxResults: maxResultsPerPage,
    });
    return queryResults;
  }
  

function generateReport_(queryResults, needHeaders) {
const rows = queryResults.rows || [];
let data = rows.map((row) => row.f.map((field) => field.v));

if (needHeaders) {
    const headers = queryResults.schema.fields.map((field) => field.name);
    data.unshift(headers);
}
return data;
}
  

function runQuery__(
    queryText,
    need_headers,
    maxResultsPerPage = 15000
  ) {

    Logger.log("Starting");
  
    const projectId = getProjectID_();
  
    const request = {
      query: queryText,
      labels: {
        "bq" : "runquery"
      },
      useLegacySql: false,
    };
  
    Logger.log("Making JOB: " + queryText);
    let queryResults = BigQuery.Jobs.query(request, projectId);
    const jobId = queryResults.jobReference.jobId;
    var jobLocation = queryResults.jobReference.location;
    // Check on status of the Query Job.
    let sleepTimeMs = 500;
    var tries = 0;
    while (!queryResults.jobComplete) {
      Logger.log("Trying " + ++tries + " time");
      Utilities.sleep(sleepTimeMs);
      sleepTimeMs *= 2;
      queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
        location: jobLocation,
        maxResults: maxResultsPerPage
      }); // ДОБАВЛЕНА ЛОКАЦИЯ
    }
    Logger.log("JOB results received");
    var headers_1 = queryResults.schema.fields.map(function (field) {
      return field.name;
    });
    const totalRows = queryResults.totalRows;
    Logger.log("Total rows: " + queryResults.totalRows);
    Logger.log("Page token: " + queryResults.pageToken);
  
    // Get all the rows of results.
    var rows = queryResults.rows;
  
    var tries = 0;
    Logger.log("Starting making rows");
  

    Logger.log("Starting pages loading");
  
    //Загружаем данные постранично
    while (queryResults.pageToken || rows != null) {
      Logger.log("Getting " + ++tries + " page");
  
      if (queryResults.pageToken) {
        Logger.log("Found page token: " + queryResults.pageToken);
  
        Logger.log("Starting page query");
        try {
          Logger.log("Step 1.");
          queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
            pageToken: queryResults.pageToken,
            location: jobLocation,
            maxResults: maxResultsPerPage
          });
        } catch(err) {
          Logger.log(`[ERROR]: ${err.message}\n${err.stack}`);
          throw new Error(err.message);
        };
  
        Logger.log("Total rows: " + queryResults.totalRows);
        Logger.log("Page token: " + queryResults.pageToken);
        Logger.log("Concating " + tries + " time");
  
        if (rows == null) {
          rows = queryResults.rows;
          Logger.log("Making rows: " + rows.length);
        } else {
          rows = rows.concat(queryResults.rows);
          Logger.log("Concating rows: " + rows.length);
        }
      }
      Logger.log("Rows received: " + rows.length);
  
      if (!rows) {
        Logger.log("No rows returned.");
        return;
      }
  
      Logger.log("Appending rows: " + rows.length);
      // Append the results.
      var data = new Array(rows.length);
      for (let i = 0; i < rows.length; i++) {
        const cols = rows[i].f;
        data[i] = new Array(cols.length);
        for (let j = 0; j < cols.length; j++) {
          data[i][j] = cols[j].v;
        }
      }
  
      rows = null;
  
      Logger.log("Setting values");
      const headers = queryResults.schema.fields.map(function (field) {
        return field.name;
      });
      Logger.log("Getting headers");
  
      if (need_headers) {
        Logger.log("Appending headers");
        // Append the headers.
        const headers = queryResults.schema.fields.map(function (field) {
          return field.name;
        });
        data.unshift(headers);
      }
  
      Logger.log("Headers: " + headers.length);

      Logger.log("Finished setting");
    }
  
    Logger.log("The End.");
  }

/**
 * Runs a BigQuery query and logs the results in a spreadsheet.
 */
function runQuery_(
  queryText,
  sheetName,
  start_col,
  start_row,
  need_headers,
  num_cols,
  maxResultsPerPage = 15000,
  delete_rows = true,
  delete_cols = false,
  with_cost_price = false
) {
  switch (sheetName) {
    case "ШК из Реализаций":
      if (start_col == null) {
        var start_col = 1;
      }
      if (num_cols == null) {
        var num_cols = 6;
      }
      if (start_row == null) {
        var start_row = 2;
      }
      if (need_headers == null) {
        var need_headers = true;
      }
      break;
    case "Брак на ВБ":
      if (need_headers == null) {
        var need_headers = false;
      }
      break;
    case "Продажи (позаказно)":
      if (start_col == null) {
        var start_col = 2;
      }
      if (num_cols == null) {
        var num_cols = 12;
      }
      if (start_row == null) {
        var start_row = 2;
      }
      if (need_headers == null) {
        var need_headers = false;
      }
      break;
    case "Список заказов":
      if (start_col == null) {
        var start_col = 2;
      }
      if (num_cols == null) {
        var num_cols = 12;
      }
      if (start_row == null) {
        var start_row = 2;
      }
      if (need_headers == null) {
        var need_headers = false;
      }
      if (with_cost_price == null) {
        var with_cost_price = false;
      }
      break;
    //Удержания и компенсации
    case "Удержания и компенсации":
      if (start_col == null) {
        var start_col = 1;
      }
      if (num_cols == null) {
        var num_cols = 7;
      }
      if (start_row == null) {
        var start_row = 7;
      }
      if (need_headers == null) {
        var need_headers = false;
      }
      break;
    case "Продажи (Озон)":
      if (start_col == null) {
        var start_col = 1;
      }
      if (num_cols == null) {
        var num_cols = 10;
      }
      if (start_row == null) {
        var start_row = 2;
      }
      if (need_headers == null) {
        var need_headers = false;
      }
      break;
    default:
      if (start_col == null) {
        var start_col = 1;
      }
      if (start_row == null) {
        var start_row = 2;
      }
      if (num_cols == null) {
        var num_cols = 12;
      }
      if (delete_rows == null) {
        var delete_rows = true;
      }
      if (delete_cols == null) {
        var delete_cols = false;
      }
      if (need_headers == null) {
        var need_headers = true;
      }
      break;
  }

  Logger.log("Starting");

  const projectId = getProjectID_();

  const request = {
    query: queryText,
    labels: {
      "bq" : "runquery"
    },
    useLegacySql: false,
  };

  Logger.log("Making JOB: " + queryText);
  let queryResults = BigQuery.Jobs.query(request, projectId);
  const jobId = queryResults.jobReference.jobId;
  var jobLocation = queryResults.jobReference.location;
  // Check on status of the Query Job.
  let sleepTimeMs = 500;
  var tries = 0;
  while (!queryResults.jobComplete) {
    Logger.log("Trying " + ++tries + " time");
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      location: jobLocation,
      maxResults: maxResultsPerPage
    }); // ДОБАВЛЕНА ЛОКАЦИЯ
  }
  Logger.log("JOB results received");
  var headers_1 = queryResults.schema.fields.map(function (field) {
    return field.name;
  });
  const totalRows = queryResults.totalRows;
  var allRows = parseInt(totalRows) + start_row - 1;
  Logger.log("Total rows: " + queryResults.totalRows);
  Logger.log("Page token: " + queryResults.pageToken);

  // Get all the rows of results.
  var rows = queryResults.rows;

  var tries = 0;
  Logger.log("Starting making rows");

  var current_start_row = start_row;
  Logger.log("Accesing spreadsheet");

  Logger.log("Get sheet by name");

  var sheets_prop = getSheetId_(ssid, sheetName);
  var sheetID = sheets_prop[0];
  var lastRowNum = sheets_prop[1];

  Logger.log("Get last row num of the sheet");
  Logger.log(
    "Last row of sheet: " + sheetName + ", Last Row Num: " + lastRowNum
  );
  Logger.log("Total Rows: " + totalRows);
  const rows_to_add = allRows - lastRowNum;
  Logger.log("To add: " + rows_to_add);

  var num_cols = headers_1.length;
  var allCols = parseInt(num_cols) + start_col - 1;
  Logger.log("Total columns: " + num_cols);

  var lastColNum = sheets_prop[2];

  Logger.log("Get last Column num of the sheet");
  Logger.log(
    "Last column of sheet: " + sheetName + ", Last column Num: " + lastColNum
  );
  Logger.log("Total Columns: " + num_cols);
  const cols_to_add = allCols - lastColNum;
  Logger.log("To add: " + cols_to_add);

  var requests = {
    requests: [],
  };
  if (start_col == 1) {
    endColumnIndex = num_cols;
  } else {
    endColumnIndex = start_col + num_cols - 1;
  }

  if (rows_to_add > 0) {
    // Добавляем строки
    requests.requests.push({
      appendDimension: {
        sheetId: sheetID,
        dimension: "ROWS",
        length: rows_to_add,
      },
    });
  } else if (rows_to_add < 0 && totalRows > 0 && delete_rows == true) {
    // Удаляем строки
    requests.requests.push({
      deleteDimension: {
        range: {
          sheetId: sheetID,
          startIndex: allRows,
          endIndex: lastRowNum,
          dimension: "ROWS",
        },
      },
    });
  }

  if (cols_to_add > 0) {
    Logger.log('Добавляем ' + cols_to_add + ' столбцов');
    // Добавляем столбцы
    requests.requests.push({
      appendDimension: {
        sheetId: sheetID,
        dimension: "COLUMNS",
        length: cols_to_add,
      },
    });
    Logger.log(requests.requests);
  }
  else if (cols_to_add < 0 && num_cols > 0 && delete_cols == true) {
    // Удаляем столбцы
    Logger.log('Удаляем ' + cols_to_add + 'столбцов');

    requests.requests.push({
      deleteDimension: {
        range: {
          sheetId: sheetID,
          startIndex: allCols,
          endIndex: lastColNum,
          dimension: "COLUMNS",
        },
      },
    });
  }

  // Очищаем только значения ячеек, оставляя форматирование
  if (sheetName === 'Настройки'){
    var clearRows = 100;
  }

  else{
    var clearRows = allRows;
  }

  requests.requests.push(
    // Очищаем фильтр листа
    {
      clearBasicFilter: {
        sheetId: sheetID,
      },
    },
    {
      updateCells: {
        range: {
          sheetId: sheetID,
          startRowIndex: start_row - 1,
          endRowIndex: clearRows,
          startColumnIndex: start_col - 1,
          endColumnIndex: endColumnIndex,
        },
        fields: "userEnteredValue", // Очищаем только значения ячеек, оставляя форматирование
      },
    }
  );

  if (sheetName === "Список заказов") {
    if (with_cost_price == true){
      var rekl_formula = constants_(ssid, sheetName, "Реклама", start_row - 1);
      if (rekl_formula != null) {
        requests.requests.push(
          {
            autoFill: {
              range: {
                sheetId: sheetID,
                startRowIndex: start_row - 1,
                endRowIndex: allRows,
                startColumnIndex: rekl_formula - 1,
                endColumnIndex: rekl_formula,
              },
            },
          }
        );
      }
    }

    else{
      var ss_formula = constants_(ssid, sheetName, "C/C", start_row - 1);
      var rekl_formula = constants_(ssid, sheetName, "Реклама", start_row - 1);
      if (ss_formula != null && rekl_formula != null) {
        requests.requests.push(
          {
            autoFill: {
              range: {
                sheetId: sheetID,
                startRowIndex: start_row - 1,
                endRowIndex: allRows,
                startColumnIndex: ss_formula - 1,
                endColumnIndex: ss_formula,
              },
            },
          },
          {
            autoFill: {
              range: {
                sheetId: sheetID,
                startRowIndex: start_row - 1,
                endRowIndex: allRows,
                startColumnIndex: rekl_formula - 1,
                endColumnIndex: rekl_formula,
              },
            },
          }
        );
      }
    }
  }


  if (sheetName === "Удержания и компенсации") {
    requests.requests.push(
      {
      updateCells: {
        range: {
          sheetId: sheetID,
          startRowIndex: 5, // Номер строки H6 - 1 (строки считаются с нуля)
          endRowIndex: 6,   // Конец диапазона (исключительно)
          startColumnIndex: 7, // Номер колонки H - 1 (колонки считаются с нуля)
          endColumnIndex: 8,   // Конец диапазона (исключительно)
        },
        rows: [
          {
            values: [
              {
                userEnteredValue: { stringValue: "С/с - компенсация = потери" },
              },
            ],
          },
        ],
        fields: "userEnteredValue",
      },
    })
  }

  Logger.log("Start batch prepare");

  try {
    var response = Sheets.Spreadsheets.batchUpdate(requests, ssid);
  } catch(err) {
    Logger.log(`[ERROR]: ${err.message}\n${err.stack}`);
    throw new Error(err.message);
  };
  Logger.log(response);
  Logger.log("Starting pages loading");

  //Загружаем данные постранично
  while (queryResults.pageToken || rows != null) {
    Logger.log("Getting " + ++tries + " page");

    if (queryResults.pageToken) {
      Logger.log("Found page token: " + queryResults.pageToken);

      Logger.log("Starting page query");
      try {
        Logger.log("Step 1.");
        queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
          pageToken: queryResults.pageToken,
          location: jobLocation,
          maxResults: maxResultsPerPage
        });
      } catch(err) {
        Logger.log(`[ERROR]: ${err.message}\n${err.stack}`);
        throw new Error(err.message);
      };

      Logger.log("Total rows: " + queryResults.totalRows);
      Logger.log("Page token: " + queryResults.pageToken);
      Logger.log("Concating " + tries + " time");

      if (rows == null) {
        rows = queryResults.rows;
        Logger.log("Making rows: " + rows.length);
      } else {
        rows = rows.concat(queryResults.rows);
        Logger.log("Concating rows: " + rows.length);
      }
    }
    Logger.log("Rows received: " + rows.length);

    if (!rows) {
      Logger.log("No rows returned.");
      return;
    }

    Logger.log("Appending rows: " + rows.length);
    // Append the results.
    var data = new Array(rows.length);
    for (let i = 0; i < rows.length; i++) {
      const cols = rows[i].f;
      data[i] = new Array(cols.length);
      for (let j = 0; j < cols.length; j++) {
        data[i][j] = cols[j].v;
      }
    }

    rows = null;

    Logger.log("Setting values");
    const headers = queryResults.schema.fields.map(function (field) {
      return field.name;
    });
    Logger.log("Getting headers");

    if (need_headers && current_start_row == start_row) {
      Logger.log("Appending headers");
      // Append the headers.
      const headers = queryResults.schema.fields.map(function (field) {
        return field.name;
      });
      data.unshift(headers);
    }

    Logger.log("Headers: " + headers.length);
    Logger.log("Getting A1 notation");
    var append_range =
      sheetName +
      "!" +
      columnToLetter_(start_col) +
      current_start_row +
      ":" +
      columnToLetter_(start_col + num_cols - 1);
    Logger.log("Getting newValueRange");
    let valueRange = data;
    Logger.log("Getting values");
    Logger.log(
      "Start updating (" +
        append_range +
        "): start_col - " +
        current_start_row +
        " data length - " +
        data.length
    );

    var request_upd = {
      valueInputOption: "USER_ENTERED",
      data: [
        {
          range: append_range,
          values: valueRange,
        },
      ],
    };
    try {
      const updateResult = Sheets.Spreadsheets.Values.batchUpdate(
        request_upd,
        ssid
      );
      Logger.log("Update result: " + updateResult);
    } catch(err) {
      Logger.log(`[ERROR]: ${err.message}\n${err.stack}`);
      throw new Error(err.message);
    };

    current_start_row = current_start_row + data.length;
    Logger.log("current_start_row " + current_start_row);
    Logger.log("Finished setting");
  }

  Logger.log("The End.");
}


/**
 * Функция для декодирования Base64 строки в Unicode с использованием Utilities в Google Apps Script.
 * @param {string} str - Строка в формате Base64.
 * @return {string} - Декодированная строка в формате Unicode.
 */
function base64DecodeUnicode_(str) {
  var decodedBytes = Utilities.base64Decode(str);
  return Utilities.newBlob(decodedBytes).getDataAsString();
}


/**
 * Функция для декодирования JWT и извлечения необходимых данных.
 * @param {string} token - JWT токен.
 */
function decodeJWT(token) {
  // Разделяем токен на его части
  var parts = token.split('.');
  var payload = parts[1];

  // Декодируем полезную нагрузку (payload)
  var decodedPayload = JSON.parse(base64DecodeUnicode_(payload));

  // Извлекаем необходимые данные
  var sid = decodedPayload.sid;
  var exp = decodedPayload.exp;
  var properties = decodedPayload.s;
  var readonly = decodedPayload.t;

  // Отображение битовых позиций на названия категорий
  var categoryMap = {
    1: "Контент",
    2: "Аналитика",
    3: "Цены и скидки",
    4: "Маркетплейс",
    5: "Статистика",
    6: "Продвижение",
    7: "Вопросы и отзывы",
    8: "Рекомендации",
    9: "Чат с покупателями",
    10: "Поставки",
    11: "Возвраты покупателями",
    12: "Документы",
    30: "Только на чтение"
  };

  // Преобразуем битовую маску в названия категорий
  var categories = [];
  for (var position in categoryMap) {
    if (properties & (1 << (position))) {  // Проверяем, что бит на данной позиции установлен
      categories.push(categoryMap[position]);
    }
  }

  // Преобразуем время жизни токена в читаемый формат
  var expDate = new Date(exp * 1000);
  var expFormatted = expDate.getFullYear() + '.' +
                     ('0' + (expDate.getMonth() + 1)).slice(-2) + '.' +
                     ('0' + expDate.getDate()).slice(-2) + ' ' +
                     ('0' + expDate.getHours()).slice(-2) + ':' +
                     ('0' + expDate.getMinutes()).slice(-2) + ':' +
                     ('0' + expDate.getSeconds()).slice(-2);

  // Логируем данные
  console.log('sid:', sid);
  console.log('Время жизни:', expFormatted);
  console.log('Категории:', categories.join(', '));
  console.log('Read-only:', readonly);
  console.log('Битовая маска:', properties.toString(2));  // Печать битовой маски в виде двоичной строки
  return [sid, expFormatted, categories.join(', ')];
}


/**
 * Преобразует строку даты в формат, используемый в BigQuery.
 *
 * @function formatDateForBigQuery_
 * @param {string} dateStr - Строка даты в формате 'день.месяц.год часы:минуты:секунды'
 * @returns {string} Строка даты в формате 'год-месяц-день часы:минуты:секунды'
 */
function formatDateForBigQuery_(dateStr) {
  // Разбиваем дату и время на составляющие
  var dateParts = dateStr.split(" ");
  var date = dateParts[0].split(".");
  var time = dateParts[1];

  // Создаем новую строку даты в нужном формате
  var formattedDate = date[0] + "-" + date[1] + "-" + date[2] + " " + time;
  return formattedDate;
}


/**
 * Получает и возвращает диагностические данные, основанные на предоставленном токене и наборе данных.
 * Собирает информацию о текущей Google таблице, скрипте, файле и пользователях.
 *
 * @function getDiagnosticData
 * @param {string} token - JWT токен, содержащий данные для декодирования.
 * @param {string} dataset - Имя набора данных.
 * @returns {Object} response - Объект, содержащий диагностические данные.
 */
function getDiagnosticData(token, dataset) {
  var response = {}

  try {
    token_data = decodeJWT(token);

    response.sid = token_data[0];
    response.expFormatted = formatDateForBigQuery_(token_data[1]);
    response.categories = token_data[2];

  } catch (err) {
    Logger.log("unable to get info about token: " + err);
  }

  try {
    response.dataset = dataset;
  } catch (err) {
    Logger.log("no dataset: " + err);
  }

  try {
    ss_id = spreadsheet.getId();
    response.ss_id = ss_id;
    ss_name = spreadsheet.getName();
    response.ss_name = ss_name;
  } catch (err) {
    Logger.log("unable to get info about spreadsheet: " + err);
  }

  try {
    var scriptId = ScriptApp.getScriptId();
    response.scriptId = scriptId;
  } catch (err) {
    Logger.log("unable to get info about script: " + err);
  }

  try{
    var file = DriveApp.getFileById(ss_id);
    var ownerEmail = file.getOwner().getEmail();
    response.ownerEmail = ownerEmail;

    var created_date = file.getDateCreated();
    response.created_date = created_date;

    var viewers = file.getViewers();

    var viewers_list = ""
    viewers.forEach(function(user) {
      viewers_list = viewers_list + user.getEmail() + ",";
    });

    response.viewers_list = viewers_list;

    var editors = file.getEditors();

    var editors_list = ""
    editors.forEach(function(user) {
      editors_list = editors_list + user.getEmail() + ",";
    });

    response.editors_list = editors_list;

    var accessType = file.getSharingAccess(); // Получаем тип доступа (общий доступ)
    var permissionType = file.getSharingPermission(); // Получаем разрешение

    var everyone_can_edit = "";
    if (accessType == DriveApp.Access.ANYONE && permissionType == DriveApp.Permission.EDIT) {
      everyone_can_edit = true;
    } else {
      everyone_can_edit = false;
    }

    response.everyone_can_edit = everyone_can_edit;
  }
  catch (err) {
    Logger.log("unable to get info about permissions: " + err);
  }

  try {
    var executorEmail = Session.getEffectiveUser().getEmail();
    response.executorEmail = executorEmail;
  } catch (err) {
    Logger.log("unable to get info about sessions: " + err);
  }

  response.cr_date = new Date();

  console.log(response);
  try {
    uploadDiagnostic_(response);
  } catch (err) {
    Logger.log("cant upload data: " + err);
  }
  return (response);
}


/**
 * Импортирует данные Списка заказов без учета опер.данных понедельно из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа.
 * @param {string} params.sheet_name - Название листа, на котором следует разместить данные.
 */
function importWBOrdersWithoutDopsWeekly(
  dataset_name,
  params = {
        sheet_name: "О файле из БД",
        start_col: 6,
        start_row: 2,
        need_headers: true,
        delete_rows: false,
        delete_cols: false
        }
) {
  const queryText =
    "SELECT date_from, date_to, REPLACE(cast(sum(payedPrice) as string), '.', ',') as payedPrice, REPLACE(cast(sum(deliveryCost) as string), '.', ',') as deliveryCost, REPLACE(cast(sum(Comission) as string), '.', ',') as Comission FROM  `fresh-gravity-322709." +
    dataset_name +
    ".sverka`  group by date_from, date_to order by date_from desc";

  runQuery(queryText, params.sheet_name, params.start_col, params.start_row, params.need_headers, undefined, undefined, params.delete_rows, params.delete_cols);
}


/**
 * Импортирует список bonus_type_name на лист Настройки из таблицы БД wb_bonusTypeName
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа.
 * @param {string} params.sheet_name - Название листа, на котором следует разместить данные.
 */
function importWBbonusTypeName(
  params = {
        sheet_name: "Настройки",
        start_col: 36,
        start_row: 6,
        need_headers: false,
        delete_rows: false,
        delete_cols: false
        }
) {
  const queryText =
    "SELECT bonus_type_name FROM  `fresh-gravity-322709.system.wb_bonusTypeName`";

  runQuery(queryText, params.sheet_name, params.start_col, params.start_row, params.need_headers, undefined, undefined, params.delete_rows, params.delete_cols);
}


function columnToLetter_(column) {
  var temp,
    letter = "";
  while (column > 0) {
    temp = (column - 1) % 26;
    letter = String.fromCharCode(temp + 65) + letter;
    column = (column - temp - 1) / 26;
  }
  return letter;
}


function getLastRowNum_(ssid, sheetname) {
  let get_range = sheetname + "!A:A";
  let a_col = Sheets.Spreadsheets.Values.get(ssid, get_range);
  var lastRow = a_col.values === undefined ? 1 : a_col.values.length;
  return lastRow;
}


function constants_(ssid, sheetname, search_string, start_row) {
  var headers = Sheets.Spreadsheets.Values.get(
    ssid,
    sheetname + "!" + start_row + ":" + start_row
  ).values[0];
  var columnIndex = headers.indexOf(search_string);
  if (columnIndex != -1) {
    var columnLetter = columnIndex + 1;
    Logger.log("# столбца: " + columnLetter);
    return columnLetter;
  } else {
    Logger.log("Столбец не найден.");
    return null;
  }
}


function getSheetId_(spreadsheetId, sheetName) {
  var spreadsheet_0 = Sheets.Spreadsheets.get(spreadsheetId);
  var sheets = spreadsheet_0.sheets;
  for (var i = 0; i < sheets.length; i++) {
    if (sheets[i].properties.title == sheetName) {
      return [
        sheets[i].properties.sheetId,
        sheets[i].properties.gridProperties.rowCount,
        sheets[i].properties.gridProperties.columnCount

      ];
    }
  }
  return undefined;
}


/**
 * Импортирует данные о поставках для БД в BigQuery из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, диапазон ячеек и отладочный режим.
 * @param {string} [params.sheet_name="Журнал поставок"] - Название листа, с которого следует взять данные для загрузки.
 * @param {string} [params.range="B7:O"] - Диапазон ячеек с данными для загрузки.
 * @param {boolean} [params.debug=false] - Флаг отладочного режима.
 * @param {number} [params.skipLeadingRows=0] - Количество пропускаемых начальных строк.
 */
function uploadWBSupplies(
  dataset_name,
  params = {
    sheet_name: "Журнал поставок",
    range: "B7:O",
    debug: true,
    skipLeadingRows: 0,
  }
) {
  // Ввод данных BigQuery в качестве переменной.
  // Набор данных
  var datasetId = dataset_name;
  // Таблица
  var tableId = "supplies";

  var writeDispositionSetting = "WRITE_TRUNCATE";

  // Название листа в Google Spreadsheet для экспорта в BigQuery:
  Logger.log(params.sheet_name);

  var sheets_prop = getSheetId_(ssid, params.sheet_name);
  var sheetID = sheets_prop[0];
  var projectId = getProjectID_();
  var file = SpreadsheetApp.getActiveSpreadsheet().getSheetById(
    sheetID
  );
  // Все данные
  var rows = file.getRange(params.range).getValues();

  // Преобразуем даты в первом столбце
  rows = rows.map(row => {
    const originalDate = row[0]; // Дата в первом столбце
    if (originalDate instanceof Date) { // Проверяем, что это объект Date
      const year = originalDate.getFullYear(); // Получаем год
      const month = String(originalDate.getMonth() + 1).padStart(2, '0'); // Получаем месяц (0-11, добавляем 1)
      const day = String(originalDate.getDate()).padStart(2, '0'); // Получаем день месяца
      row[0] = `${year}-${month}-${day}`; // Преобразуем в формат гггг-мм-дд
    }
    return row; // Возвращаем строку с обновленной датой
  });

  var columnsToExtract = [0, 1, 2, 13, 11]; // Индексы столбцов (считаются с 0)

  // Формируем итоговый массив
  var resultArray = rows.map(row => columnsToExtract.map(col => row[col]));
  var rowsCSV = resultArray.join("\n");
  var blob = Utilities.newBlob(rowsCSV, "text/csv");
  var data = blob.setContentType("application/octet-stream");
  if (params.debug) {
    Logger.log(rowsCSV);
  }

  // Создание задания на загрузку данных.
  var job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: projectId,
          datasetId: datasetId,
          tableId: tableId,
        },
        skipLeadingRows: params.skipLeadingRows,
        writeDisposition: writeDispositionSetting,
      },
      labels: {
        "bq" : "uploadwbsupplies"
      },
    },
  };
  if (params.debug) {
    Logger.log(job);
  }

  // Отправка задания в BigQuery для выполнения запроса.
  var runJob = BigQuery.Jobs.insert(job, projectId, data);
  var jobId = runJob.jobReference.jobId;
  Logger.log("jobId: " + jobId);
  Logger.log(runJob.status);
  Logger.log("FINISHED!");
}


// экранирует запятые
function escapeCommas(data) {
  return data.map(row => row.map(value => {
    if (typeof value === 'string' && value.includes(',')) {
      return `"${value}"`;
    }
    return value;
  }));
}


/**
 * Импортирует данные о платной приемке из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, начальные координаты, наличие заголовков и количество колонок.
 * @param {string} [params.sheet_name="Платная приемка (исх.)"] - Название листа, на котором следует разместить данные.
 * @param {number} [params.start_col=1] - Начальная колонка для размещения данных.
 * @param {number} [params.start_row=1] - Начальная строка для размещения данных.
 * @param {boolean} [params.need_headers=true] - Наличие заголовков в результирующей таблице.
 * @param {number} [params.num_cols=8] - Количество колонок данных.
 * @param {string} [params.start_date=null] - Начальная дата для фильтрации данных.
 */
function importWBAcceptance(
  dataset_name,
  params = {
    sheet_name: "Платная приемка (исх.)",
    start_col: 1,
    start_row: 1,
    need_headers: true,
    num_cols: 8
  }
) {

  var sheets_prop = getSheetId_(ssid, params.sheet_name);

  if (sheets_prop === undefined){
    Logger.log('Создаем лист ' + params.sheet_name);
    var newSheet = spreadsheet.insertSheet(params.sheet_name);
    var totalRows = newSheet.getMaxRows();
    var totalColumns = newSheet.getMaxColumns();

    if (totalRows > 20) {
      newSheet.deleteRows(21, totalRows - 20);
    }
    if (totalColumns > 8) {
      newSheet.deleteColumns(9, totalColumns - 8);
    }

    if (newSheet) {
      Logger.log("Лист " + params.sheet_name + " успешно создан");
    }
  }

  var sheetID = sheets_prop[0];
  var file = SpreadsheetApp.getActiveSpreadsheet().getSheetById(
    sheetID
  );
  file.hideSheet();

  var sheets_settings_prop = getSheetId_(ssid, 'Настройки');
  var sheets_settings_id = sheets_settings_prop[0];
  var sheet_settings = spreadsheet.getSheetById(sheets_settings_id);
  var settings_start_date = sheet_settings.getRange('D4').getValue();
  settings_start_date = Utilities.formatDate(settings_start_date, "GMT+3", 'yyyy-MM-dd');

  let start_date_query_part = "";
  if (typeof settings_start_date !== "undefined" && settings_start_date !== null) {
    start_date_query_part = 'and date(shk_create_date) >= "' + settings_start_date + '" ';
  }

  const query_text =
    "select gi_create_date as create_date, shk_create_date as acceptance_date, income_id, nm_id, brand_name, count, REPLACE(cast(total as string), '.', ',') as total, subject_name from `fresh-gravity-322709." +
    dataset_name +
    ".acceptance` where income_id is not null " +
    start_date_query_part +
    "order by create_date desc";

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}


/**
 * Импортирует данные о платной приемке из указанного набора данных.
 * @param {string} dataset_name - Название набора данных для выполнения запроса.
 * @param {Object} params - Параметры запроса, включая название листа, начальные координаты, наличие заголовков и количество колонок.
 * @param {string} [params.sheet_name="Хранение (исх.)"] - Название листа, на котором следует разместить данные.
 * @param {number} [params.start_col=1] - Начальная колонка для размещения данных.
 * @param {number} [params.start_row=1] - Начальная строка для размещения данных.
 * @param {boolean} [params.need_headers=true] - Наличие заголовков в результирующей таблице.
 * @param {number} [params.num_cols=8] - Количество колонок данных.
 * @param {string} [params.start_date=null] - Начальная дата для фильтрации данных.
 */
function importWBStorage(
  dataset_name,
  params = {
    sheet_name: "Хранение (исх.)",
    start_col: 1,
    start_row: 1,
    need_headers: true,
    num_cols: 8
  }
) {

  var sheets_prop = getSheetId_(ssid, params.sheet_name);

  if (sheets_prop === undefined){
    Logger.log('Создаем лист ' + params.sheet_name);
    var newSheet = spreadsheet.insertSheet(params.sheet_name);
    var totalRows = newSheet.getMaxRows();
    var totalColumns = newSheet.getMaxColumns();

    if (totalRows > 20) {
      newSheet.deleteRows(21, totalRows - 20);
    }
    if (totalColumns > 8) {
      newSheet.deleteColumns(9, totalColumns - 8);
    }

    if (newSheet) {
      Logger.log("Лист " + params.sheet_name + " успешно создан");
    }
  }

  var sheetID = sheets_prop[0];
  var file = SpreadsheetApp.getActiveSpreadsheet().getSheetById(
    sheetID
  );
  file.hideSheet();

  var sheets_settings_prop = getSheetId_(ssid, 'Настройки');
  var sheets_settings_id = sheets_settings_prop[0];
  var sheet_settings = spreadsheet.getSheetById(sheets_settings_id);
  var settings_start_date = sheet_settings.getRange('D4').getValue();
  settings_start_date = Utilities.formatDate(settings_start_date, "GMT+3", 'yyyy-MM-dd');

  let start_date_query_part = "";
  if (typeof settings_start_date !== "undefined" && settings_start_date !== null) {
    start_date_query_part = 'and date >= "' + settings_start_date + '" ';
  }

  const query_text =
    "select date(date) as date, barcode, brand, vendor_code as sa_name, size as ts_name, nm_id, subject as category, REPLACE(cast(sum(warehouse_price) as string), '.', ',') as sum from `fresh-gravity-322709." +
    dataset_name +
    ".paid_storage` where gi_id is not null " +
    start_date_query_part +
    "group by date, barcode, brand, vendor_code, size, nm_id, subject order by date desc";

  runQuery(
    query_text,
    params.sheet_name,
    params.start_col,
    params.start_row,
    params.need_headers,
    params.num_cols
  );
}


function getStocksData(token){
  WBAPI.getStocksData('Склад (исх.)','https://statistics-api.wildberries.ru/api/v1/supplier/stocks',token)
}