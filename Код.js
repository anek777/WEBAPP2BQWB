function doGet(e) {
    Logger.log("Hello!");
}
/**
 * Веб-приложение для обработки запросов от библиотеки BQ и возврата результатов.
 */

/**
 * Список авторизованных пользователей и их таблиц.
 * В будущем заменить на запрос в базу данных.
 */
var AUTHORIZED_USERS = [
    { email: 'anek777@gmail.com', ssid: '1XcwY6x4feaRu8RU4mfupqPGo620JDqdfZ_e0jZfiBh8' },
    { email: 'user2@example.com', ssid: '2wxyz91011mnop1213qrst' }
];

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
            return createResponse(false, 'POST-запрос не содержит данных.');
        }

        // Парсим содержимое запроса
        var requestBody = JSON.parse(e.postData.contents);
        var functionName = requestBody.functionName; // Имя функции для вызова
        var parameters = requestBody.parameters || []; // Аргументы вызова
        var ssid = requestBody.ssid; // ID таблицы, из которой был вызов
        var userEmail = requestBody.userEmail; // Почта пользователя

        // Проверяем авторизацию пользователя
        if (!isAuthorized(userEmail, ssid)) {
            return createResponse(false, 'Пользователь или таблица не авторизованы.');
        }

        // Проверяем существование функции в библиотеке BQ
        if (BQ[functionName] && typeof BQ[functionName] === 'function') {
            // Выполняем функцию и получаем результат
            var result = BQ[functionName](...parameters);
            return createResponse(true, 'Функция выполнена успешно.', result);
        } else {
            return createResponse(false, 'Указанная функция не найдена в библиотеке.');
        }

    } catch (error) {
        return createResponse(false, 'Произошла ошибка: ' + error.message);
    }
}

/**
 * Проверка авторизации пользователя и таблицы.
 *
 * @param {string} email - Электронная почта пользователя.
 * @param {string} ssid - Идентификатор таблицы.
 * @return {boolean} - Возвращает true, если пользователь и таблица авторизованы.
 */
function isAuthorized(email, ssid) {
    return AUTHORIZED_USERS.some(function (user) {
        return user.email === email && user.ssid === ssid;
    });
}

/**
 * Создание стандартизированного ответа.
 *
 * @param {boolean} success - Успешность выполнения.
 * @param {string} message - Сообщение.
 * @param {Object} [data=null] - Дополнительные данные.
 * @return {ContentService.TextOutput} - Ответ в формате JSON.
 */
function createResponse(success, message, data) {
    var response = {
        success: success,
        message: message,
        data: data || null
    };
    return ContentService.createTextOutput(JSON.stringify(response))
        .setMimeType(ContentService.MimeType.JSON);
}
