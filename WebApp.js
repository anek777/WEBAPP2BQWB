/**
 * Веб-приложение для обработки запросов от библиотеки BQ и возврата результатов.
 */

function doGet(e) {
    Logger.log("GET-запрос получен: " + JSON.stringify(e));
    return ContentService.createTextOutput('Hello from doGet!');
}

/**
 * Главный обработчик POST-запросов.
 *
 * @param {Object} e - Объект запроса HTTP POST, содержащий входные данные.
 * @return {ContentService.TextOutput} - Ответ в формате JSON.
 */
function doPost(e) {
    try {
        // Проверяем, что есть данные в запросе
        if (!e.postData || !e.postData.contents) {
            Logger.log('POST-запрос не содержит данных.');
            return createResponse_(false, 'POST-запрос не содержит данных.');
        }

        // Парсим содержимое запроса
        var requestBody = JSON.parse(e.postData.contents);
        Logger.log('Данные POST-запроса: ' + JSON.stringify(requestBody));
        var functionName = requestBody.functionName; // Имя функции для вызова
        var parameters = requestBody.parameters || []; // Аргументы вызова
        var ssid = requestBody.ssid; // ID таблицы, из которой был вызов
        var userEmail = requestBody.userEmail; // Почта пользователя

        // Проверяем авторизацию пользователя
        if (!isAuthorized_(userEmail, ssid)) {
            Logger.log('Пользователь или таблица не авторизованы: ' + userEmail);
            return createResponse_(false, 'Пользователь или таблица не авторизованы.');
        }

        // Проверяем существование функции в разрешенном списке
        if (!isFunctionAuthorized_(functionName)) {
            Logger.log('Функция не входит в разрешенный список: ' + functionName);
            return createResponse_(false, 'Функция не входит в разрешенный список.');
        }

        // Проверяем наличие функции в текущей среде и выполняем
        if (typeof this[functionName] === 'function') {
            Logger.log('Выполнение функции: ' + functionName);
            var result = this[functionName](...parameters);
            return createResponse_(true, 'Функция выполнена успешно.', result);
        } else {
            Logger.log('Функция не найдена: ' + functionName);
            return createResponse_(false, 'Указанная функция не найдена.');
        }

    } catch (error) {
        Logger.log('Произошла ошибка: ' + error.message);
        return createResponse_(false, 'Произошла ошибка: ' + error.message);
    }
}

/**
 * Проверка авторизации пользователя и таблицы.
 *
 * @param {string} email - Электронная почта пользователя.
 * @param {string} ssid - Идентификатор таблицы.
 * @return {boolean} - Возвращает true, если пользователь и таблица авторизованы.
 */
function isAuthorized_(email, ssid) {
    return AUTHORIZED_USERS.some(function (user) {
        return user.email === email && user.ssid === ssid;
    });
}

/**
 * Проверка, входит ли функция в разрешенный список.
 *
 * @param {string} functionName - Имя функции, которую нужно вызвать.
 * @return {boolean} - Возвращает true, если функция разрешена для вызова.
 */
function isFunctionAuthorized_(functionName) {
    return AUTHORIZED_FUNCTIONS.includes(functionName);
}

/**
 * Создание стандартизированного ответа.
 *
 * @param {boolean} success - Успешность выполнения.
 * @param {string} message - Сообщение.
 * @param {Object} [data=null] - Дополнительные данные.
 * @return {ContentService.TextOutput} - Ответ в формате JSON.
 */
function createResponse_(success, message, data) {
    var response = {
        success: success,
        message: message,
        data: data || null
    };
    return ContentService.createTextOutput(JSON.stringify(response))
        .setMimeType(ContentService.MimeType.JSON);
}
