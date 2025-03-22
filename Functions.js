function getProjectID_() {
  return (projectId = "fresh-gravity-322709");
}

/**
 * Импортирует список ссылок для картинок на лист Настройки из таблицы БД wb_baskets
 * @param {Object} params - Параметры запроса, включая название листа.
 */
function importWBbaskets() {
    const queryText =
      "SELECT nm_id, link FROM  `fresh-gravity-322709.system.wb_baskets`";
    return runQuery(queryText);
  }
  
  function runQuery(queryText, maxResultsPerPage = 15000, pageToken = null) {
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
  
    const data = generateReport_(queryResults); 
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
  

function generateReport_(queryResults) {
  const rows = queryResults.rows || [];
  const data = rows.map((row) => row.f.map((field) => field.v));
  const headers = queryResults.schema.fields.map((field) => field.name);

  return {
    headers: headers,
    data: data,
  };
}