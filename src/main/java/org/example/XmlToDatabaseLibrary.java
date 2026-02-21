package org.example;

import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.GPathResult;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.*;

/**
 * Утилитарный класс для динамического парсинга XML каталога и обновления БД.
 * Предназначен для использования в качестве библиотеки.
 */
public class XmlToDatabaseLibrary {

    private final String xmlUrl;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    /**
     * Создаёт экземпляр библиотеки с заданными параметрами.
     *
     * @param xmlUrl     URL XML-каталога (в тестовом задании, "https://expro.ru/bitrix/catalog_export/export_Sai.xml")
     * @param dbUrl      JDBC URL для подключения к PostgreSQL (для тестового задания, "jdbc:postgresql://localhost:5432/postgres")
     * @param dbUser     имя пользователя базы данных
     * @param dbPassword пароль пользователя базы данных
     */
    public XmlToDatabaseLibrary(String xmlUrl, String dbUrl, String dbUser, String dbPassword) {
        this.xmlUrl = xmlUrl;
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    /**
     * Конструктор по умолчанию, читающий параметры из переменных окружения.
     * Переменные: XML_URL, DB_URL, DB_USER, DB_PASSWORD.
     * Если переменная не задана, используется значение по умолчанию (для тестов).
     */
    public XmlToDatabaseLibrary() {
        this(
                System.getenv().getOrDefault("XML_URL", "https://expro.ru/bitrix/catalog_export/export_Sai.xml"),
                System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://localhost:5432/postgres"),
                System.getenv().getOrDefault("DB_USER", "postgres"),
                System.getenv().getOrDefault("DB_PASSWORD", "etalon")
        );
    }

    /**
     * Получает корневой элемент XML-документа после парсинга.
     * Настройки парсера:
     *     - DOCTYPE разрешён, но загрузка внешних сущностей отключена.
     *     - Запрос shops.dtd игнорируется через пустой EntityResolver.
     *
     * @return корневой элемент XML в виде {@link GPathResult}
     * @throws IOException                  если ошибка чтения по URL
     * @throws SAXException                 если ошибка парсинга XML
     * @throws ParserConfigurationException если ошибка конфигурации парсера
     */
    private GPathResult getXmlRoot() throws IOException, SAXException, ParserConfigurationException {
        try (InputStream is = new URL(xmlUrl).openStream()) {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
            spf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            spf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

            SAXParser parser = spf.newSAXParser();
            XMLReader xmlReader = parser.getXMLReader();

            xmlReader.setEntityResolver((publicId, systemId) -> {
                if (systemId != null && systemId.endsWith("shops.dtd")) {
                    return new InputSource(new java.io.StringReader(""));
                }
                return null;
            });

            XmlSlurper slurper = new XmlSlurper(xmlReader);

            GPathResult root = slurper.parse(is);
            if (root.children().size() == 1) {
                Object firstChild = root.children().iterator().next();
                if (firstChild instanceof GPathResult) {
                    GPathResult possibleShop = (GPathResult) firstChild;
                    if ("shop".equalsIgnoreCase(possibleShop.name())) {
                        return possibleShop;
                    }
                }
            }
            return root;
        }
    }

    /**
     * Возвращает список названий основных таблиц, поддерживаемых библиотекой.
     *
     * @return список {@code ["currencies", "categories", "offers"]}
     */
    public List<String> getTableNames() {
        try {
            GPathResult root = getXmlRoot();
            GPathResult targetLevel = root;
            if (root.children().size() == 1) {
                Object firstChild = root.children().iterator().next();
                if (firstChild instanceof GPathResult) {
                    GPathResult possibleContainer = (GPathResult) firstChild;
                    if (possibleContainer.children().size() > 0) {
                        targetLevel = possibleContainer;
                    }
                }
            }

            Set<String> tableNames = new LinkedHashSet<>();
            Iterator<Object> iterator = targetLevel.children().iterator();
            while (iterator.hasNext()) {
                Object child = iterator.next();
                if (child instanceof GPathResult) {
                    GPathResult childElem = (GPathResult) child;
                    if (childElem.children().size() > 0) {
                        tableNames.add(childElem.name());
                    }
                }
            }

            if (tableNames.isEmpty()) {
                return Arrays.asList("currencies", "categories", "offers");
            }
            return new ArrayList<>(tableNames);
        } catch (Exception e) {
            System.err.println("Warning: Failed to parse XML for table names, using default list. " + e.getMessage());
            return Arrays.asList("currencies", "categories", "offers");
        }
    }

    /**
     * Генерирует SQL-скрипт (DDL) для создания указанной таблицы.
     * Скрипт включает определение первичных ключей, уникальных ограничений и индексов.
     *
     * @param tableName имя таблицы (регистр не важен)
     * @return строка с SQL-командой {@code CREATE TABLE IF NOT EXISTS ...}
     * @throws RuntimeException если передано неизвестное имя таблицы или произошла ошибка
     */
    public String getTableDDL(String tableName) {
        try {
            switch (tableName.toLowerCase()) {
                case "currencies":
                    return generateCurrencyDDL();
                case "categories":
                    return generateCategoriesDDL();
                case "offers":
                    return generateOffersDDL();
                default:
                    return "-- Unknown table name: " + tableName;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate DDL for table: " + tableName, e);
        }
    }

    /**
     * Генерирует DDL для таблицы currency.
     *
     * @return SQL-скрипт создания таблицы currency
     */
    private String generateCurrencyDDL() {
        return "CREATE TABLE IF NOT EXISTS currencies (\n" +
                "    id SERIAL PRIMARY KEY,\n" +
                "    currency_id VARCHAR(10) UNIQUE,\n" +
                "    code VARCHAR(10),\n" +
                "    name TEXT,\n" +
                "    rate NUMERIC\n" +
                ");";
    }

    /**
     * Генерирует DDL для таблицы categories.
     *
     * @return SQL-скрипт создания таблицы categories
     */
    private String generateCategoriesDDL() {
        return "CREATE TABLE IF NOT EXISTS categories (\n" +
                "    id SERIAL PRIMARY KEY,\n" +
                "    category_id INTEGER UNIQUE,\n" +
                "    name TEXT,\n" +
                "    parent_id INTEGER\n" +
                ");";
    }

    /**
     * Генерирует DDL для таблицы offers и связанной таблицы offer_attributes.
     *
     * @return SQL-скрипт создания таблиц offers и offer_attributes с индексами
     */
    private String generateOffersDDL() {
        return "CREATE TABLE IF NOT EXISTS offers (\n" +
                "    id SERIAL PRIMARY KEY,\n" +
                "    offer_id VARCHAR(50) UNIQUE,\n" +
                "    vendor_code VARCHAR(100) UNIQUE,\n" +
                "    available BOOLEAN,\n" +
                "    name TEXT,\n" +
                "    price NUMERIC,\n" +
                "    currency_id VARCHAR(10),\n" +
                "    category_id INTEGER,\n" +
                "    picture TEXT,\n" +
                "    description TEXT\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS offer_attributes (\n" +
                "    id SERIAL PRIMARY KEY,\n" +
                "    offer_vendor_code VARCHAR(100) REFERENCES offers(vendor_code),\n" +
                "    param_name VARCHAR(255),\n" +
                "    param_value TEXT\n" +
                ");\n" +
                "\n" +
                "CREATE INDEX IF NOT EXISTS idx_offer_attributes_vendor_code ON offer_attributes(offer_vendor_code);";
    }

    /**
     * Последовательно обновляет все таблицы (currency, categories, offers) на основе данных из XML.
     *
     * @throws SQLException при ошибках подключения к БД, парсинга XML или выполнения запросов
     */
    public void update() throws SQLException {
        for (String tableName : getTableNames()) {
            update(tableName);
        }
    }

    /**
     * Обновляет конкретную таблицу по её имени.
     *
     * Данные извлекаются из XML и синхронизируются с БД с использованием операций
     * {@code INSERT ... ON CONFLICT} (upsert). Для таблицы {@code offers} также обрабатываются
     * дочерние элементы {@code <param>}, которые сохраняются в связанную таблицу {@code offer_attributes}.
     *
     * @param tableName имя таблицы (currency, categories или offers)
     * @throws SQLException              при ошибках подключения к БД, парсинга XML или выполнения запросов
     * @throws IllegalArgumentException если передано неизвестное имя таблицы
     */
    public void update(String tableName) throws SQLException {
        GPathResult root;
        try {
            root = getXmlRoot();
        } catch (IOException | SAXException | ParserConfigurationException e) {
            throw new SQLException("Failed to parse XML", e);
        }

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            conn.setAutoCommit(false);
            try {
                switch (tableName.toLowerCase()) {
                    case "currencies":
                        updateCurrency(conn, root);
                        break;
                    case "categories":
                        updateCategories(conn, root);
                        break;
                    case "offers":
                        updateOffers(conn, root);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown table: " + tableName);
                }
                conn.commit();
                System.out.println("Table " + tableName + " updated successfully.");
            } catch (Exception e) {
                conn.rollback();
                throw new SQLException("Failed to update table: " + tableName, e);
            }
        }
    }

    /**
     * Обновляет таблицу currency данными из XML.
     *
     * @param conn  соединение с БД
     * @param root  корневой элемент XML
     * @throws SQLException при ошибках выполнения SQL
     */
    @SuppressWarnings("unchecked")
    private void updateCurrency(Connection conn, GPathResult root) throws SQLException {
        createTableIfNotExists(conn, "currencies", generateCurrencyDDL());

        String upsertSql = "INSERT INTO currencies (currency_id, code, name, rate) VALUES (?, ?, ?, ?) " +
                "ON CONFLICT (currency_id) DO UPDATE SET code = EXCLUDED.code, " +
                "name = EXCLUDED.name, rate = EXCLUDED.rate";

        try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
            Object currenciesObj = root.getProperty("currencies");
            if (currenciesObj == null) {
                System.out.println("No <currencies> container found.");
                return;
            }
            if (!(currenciesObj instanceof GPathResult)) {
                System.out.println("currencies is not a GPathResult: " + currenciesObj.getClass());
                return;
            }
            GPathResult currencies = (GPathResult) currenciesObj;
            Object currencyListObj = currencies.getProperty("currency");
            if (!(currencyListObj instanceof GPathResult)) {
                System.out.println("No <currency> elements inside <currencies>.");
                return;
            }
            GPathResult currencyList = (GPathResult) currencyListObj;
            int count = 0;
            Iterator<Object> iterator = currencyList.iterator();
            while (iterator.hasNext()) {
                Object obj = iterator.next();
                if (!(obj instanceof GPathResult)) continue;
                GPathResult currency = (GPathResult) obj;

                String id = getAttribute(currency, "id");
                if (id == null || id.isEmpty()) {
                    System.out.println("Skipping currency with missing id");
                    continue;
                }

                String code = getAttribute(currency, "code");
                if (code == null || code.isEmpty()) {
                    code = id;
                }

                String name = getChildText(currency, "name");
                if (name == null || name.isEmpty()) {
                    name = getAttribute(currency, "name");
                }
                if (name == null) name = "";

                String rateStr = getAttribute(currency, "rate");
                BigDecimal rate = parseBigDecimal(rateStr);

                ps.setString(1, id);
                ps.setString(2, code);
                ps.setString(3, name);
                ps.setBigDecimal(4, rate);
                ps.addBatch();
                count++;
            }
            System.out.println("Found " + count + " currency entries.");
            if (count > 0) {
                int[] results = ps.executeBatch();
                System.out.println("Inserted/updated " + results.length + " currencies.");
            }
        }
    }

    /**
     * Обновляет таблицу categories данными из XML.
     *
     * @param conn  соединение с БД
     * @param root  корневой элемент XML
     * @throws SQLException при ошибках выполнения SQL
     */
    @SuppressWarnings("unchecked")
    private void updateCategories(Connection conn, GPathResult root) throws SQLException {
        createTableIfNotExists(conn, "categories", generateCategoriesDDL());

        String upsertSql = "INSERT INTO categories (category_id, name, parent_id) VALUES (?, ?, ?) " +
                "ON CONFLICT (category_id) DO UPDATE SET name = EXCLUDED.name, " +
                "parent_id = EXCLUDED.parent_id";

        try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
            Object categoriesContainerObj = root.getProperty("categories");
            if (categoriesContainerObj == null) {
                System.out.println("No <categories> container found at current level.");
                return;
            }
            if (!(categoriesContainerObj instanceof GPathResult)) {
                System.out.println("categories is not a GPathResult, it's: " + categoriesContainerObj.getClass());
                return;
            }
            GPathResult categoriesContainer = (GPathResult) categoriesContainerObj;

            Object categoryListObj = categoriesContainer.getProperty("category");
            if (categoryListObj == null) {
                System.out.println("No <category> elements found inside <categories>.");
                return;
            }
            if (!(categoryListObj instanceof GPathResult)) {
                System.out.println("category list is not a GPathResult, it's: " + categoryListObj.getClass());
                return;
            }
            GPathResult categoryList = (GPathResult) categoryListObj;

            Iterator<Object> iterator = categoryList.iterator();
            while (iterator.hasNext()) {
                Object obj = iterator.next();
                if (!(obj instanceof GPathResult)) {
                    System.out.println("Skipping non-GPathResult element: " + obj.getClass());
                    continue;
                }
                GPathResult category = (GPathResult) obj;

                String idStr = getAttribute(category, "id");
                if (idStr == null || idStr.isEmpty()) {
                    System.out.println("Skipping category with missing id");
                    continue;
                }
                Integer id = parseInt(idStr, null);
                if (id == null) {
                    System.out.println("Skipping category with invalid id: " + idStr);
                    continue;
                }

                String name = category.text();
                if (name == null) {
                    name = "";
                }

                String parentIdStr = getAttribute(category, "parentId");
                Integer parentId = parseInt(parentIdStr, null);

                ps.setInt(1, id);
                ps.setString(2, name);
                if (parentId != null) {
                    ps.setInt(3, parentId);
                } else {
                    ps.setNull(3, Types.INTEGER);
                }
                ps.addBatch();
            }
        }
    }

    /**
     * Обновляет таблицы offers и offer_attributes данными из XML.
     *
     * @param conn  соединение с БД
     * @param root  корневой элемент XML
     * @throws SQLException при ошибках выполнения SQL
     */
    @SuppressWarnings("unchecked")
    private void updateOffers(Connection conn, GPathResult root) throws SQLException {
        createTableIfNotExists(conn, "offers", generateOffersDDL());

        String deleteAttributesSql = "DELETE FROM offer_attributes WHERE offer_vendor_code = ?";
        String upsertOfferSql = "INSERT INTO offers (offer_id, vendor_code, available, name, price, " +
                "currency_id, category_id, picture, description) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (vendor_code) DO UPDATE SET " +
                "offer_id = EXCLUDED.offer_id, available = EXCLUDED.available, " +
                "name = EXCLUDED.name, price = EXCLUDED.price, " +
                "currency_id = EXCLUDED.currency_id, category_id = EXCLUDED.category_id, " +
                "picture = EXCLUDED.picture, description = EXCLUDED.description";
        String insertAttributeSql = "INSERT INTO offer_attributes (offer_vendor_code, param_name, param_value) " +
                "VALUES (?, ?, ?)";

        try (PreparedStatement psOffer = conn.prepareStatement(upsertOfferSql);
             PreparedStatement psDeleteAttr = conn.prepareStatement(deleteAttributesSql);
             PreparedStatement psInsertAttr = conn.prepareStatement(insertAttributeSql)) {

            Object offersContainerObj = root.getProperty("offers");
            if (offersContainerObj == null) {
                System.out.println("No <offers> container found.");
                return;
            }
            if (!(offersContainerObj instanceof GPathResult)) {
                System.out.println("offers is not a GPathResult: " + offersContainerObj.getClass());
                return;
            }
            GPathResult offersContainer = (GPathResult) offersContainerObj;

            Object offerListObj = offersContainer.getProperty("offer");
            if (offerListObj == null) {
                System.out.println("No <offer> elements inside <offers>.");
                return;
            }
            if (!(offerListObj instanceof GPathResult)) {
                System.out.println("offer list is not a GPathResult: " + offerListObj.getClass());
                return;
            }
            GPathResult offerList = (GPathResult) offerListObj;

            Iterator<Object> iterator = offerList.iterator();
            while (iterator.hasNext()) {
                Object obj = iterator.next();
                if (!(obj instanceof GPathResult)) continue;
                GPathResult offer = (GPathResult) obj;

                String vendorCode = getChildText(offer, "vendorCode");
                if (vendorCode == null || vendorCode.isEmpty()) {
                    System.out.println("Skipping offer with missing vendorCode");
                    continue;
                }

                String offerId = getAttribute(offer, "id");
                String available = getAttribute(offer, "available");
                String name = getChildText(offer, "name");
                String price = getChildText(offer, "price");
                String currencyId = getChildText(offer, "currencyId");
                String categoryId = getChildText(offer, "categoryId");
                String picture = getChildText(offer, "picture");
                String description = getChildText(offer, "description");

                psOffer.setString(1, offerId);
                psOffer.setString(2, vendorCode);
                psOffer.setBoolean(3, "true".equalsIgnoreCase(available));
                psOffer.setString(4, name);
                psOffer.setBigDecimal(5, parseBigDecimal(price));
                psOffer.setString(6, currencyId);
                psOffer.setInt(7, parseInt(categoryId, 0));
                psOffer.setString(8, picture);
                psOffer.setString(9, description);
                psOffer.addBatch();

                psDeleteAttr.setString(1, vendorCode);
                psDeleteAttr.addBatch();

                Object paramsObj = offer.getProperty("param");
                if (paramsObj instanceof GPathResult) {
                    GPathResult params = (GPathResult) paramsObj;
                    Iterator<Object> paramIterator = params.iterator();
                    while (paramIterator.hasNext()) {
                        Object paramObj = paramIterator.next();
                        if (!(paramObj instanceof GPathResult)) continue;
                        GPathResult param = (GPathResult) paramObj;
                        String paramName = getAttribute(param, "name");
                        String paramValue = param.text();

                        psInsertAttr.setString(1, vendorCode);
                        psInsertAttr.setString(2, paramName);
                        psInsertAttr.setString(3, paramValue);
                        psInsertAttr.addBatch();
                    }
                }
            }
        }
    }

    /**
     * Создаёт таблицу, если она ещё не существует.
     *
     * @param conn      соединение с БД
     * @param tableName имя таблицы
     * @param ddl       SQL-скрипт создания таблицы
     * @throws SQLException при ошибках выполнения SQL
     */
    private void createTableIfNotExists(Connection conn, String tableName, String ddl) throws SQLException {
        String checkSql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ?)";
        try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
            ps.setString(1, tableName);
            ResultSet rs = ps.executeQuery();
            rs.next();
            if (!rs.getBoolean(1)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(ddl);
                }
            }
        }
    }

    /**
     * Возвращает список названий колонок указанной таблицы.
     *
     * @param tableName имя таблицы
     * @return список строк с именами колонок (в нижнем регистре)
     * @throws SQLException при ошибках подключения или выполнения запроса
     */
    public List<String> getColumnNames(String tableName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            return new ArrayList<>(getTableColumns(conn, tableName));
        }
    }

    /**
     * Получает множество названий колонок таблицы из метаданных.
     *
     * @param conn      соединение с БД
     * @param tableName имя таблицы
     * @return множество имён колонок в нижнем регистре
     * @throws SQLException при ошибках доступа к метаданным
     */
    private Set<String> getTableColumns(Connection conn, String tableName) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME").toLowerCase());
            }
        }
        return columns;
    }

    /**
     * Проверяет, является ли указанная колонка частью уникального индекса (т.е. служит уникальным идентификатором).
     *
     * @param tableName  имя таблицы
     * @param columnName имя колонки
     * @return {@code true}, если колонка входит в уникальный индекс; иначе {@code false}
     * @throws SQLException при ошибках доступа к метаданным БД
     */
    public boolean isColumnId(String tableName, String columnName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            try (ResultSet rs = conn.getMetaData().getIndexInfo(null, null, tableName, true, true)) {
                while (rs.next()) {
                    if (columnName.equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Генерирует SQL-скрипт для добавления новых колонок в таблицу, если в ней отсутствуют некоторые ожидаемые поля.
     *
     * Ожидаемые поля жёстко заданы для таблиц {@code currency} и {@code categories}.
     * Для таблицы {@code offers} возвращает комментарий, что изменения структуры обрабатываются через связанную таблицу атрибутов.
     *
     * @param tableName имя таблицы
     * @return строка с командой {@code ALTER TABLE ... ADD COLUMN ...} или комментарий об отсутствии изменений
     * @throws SQLException при ошибках получения метаданных
     */
    public String getDDLChange(String tableName) throws SQLException {
        if ("offers".equalsIgnoreCase(tableName)) {
            return "-- Offers structure changes are handled via offer_attributes table.";
        }

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            Set<String> dbColumns = getTableColumns(conn, tableName);
            Set<String> expectedColumns = getExpectedColumns(tableName);

            expectedColumns.removeAll(dbColumns);
            if (expectedColumns.isEmpty()) {
                return "-- No changes needed";
            }

            StringBuilder alterSql = new StringBuilder("ALTER TABLE " + tableName + "\n");
            for (String newCol : expectedColumns) {
                alterSql.append("    ADD COLUMN ").append(newCol).append(" TEXT,\n");
            }
            alterSql.setLength(alterSql.length() - 2);
            alterSql.append(";");
            return alterSql.toString();
        }
    }

    /**
     * Возвращает ожидаемый набор колонок для заданной таблицы.
     *
     * @param tableName имя таблицы
     * @return множество имён колонок
     */
    private Set<String> getExpectedColumns(String tableName) {
        Set<String> columns = new HashSet<>();
        switch (tableName.toLowerCase()) {
            case "currencies":
                columns.addAll(Arrays.asList("id", "currency_id", "code", "name", "rate"));
                break;
            case "categories":
                columns.addAll(Arrays.asList("id", "category_id", "name", "parent_id"));
                break;
        }
        return columns;
    }

    /**
     * Возвращает значение атрибута узла XML.
     *
     * @param node узел GPathResult
     * @param name имя атрибута
     * @return строковое значение атрибута или пустая строка, если атрибут отсутствует или произошла ошибка
     */
    private String getAttribute(GPathResult node, String name) {
        try {
            Object value = node.getProperty("@" + name);
            return value != null ? value.toString() : "";
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Возвращает текст дочернего элемента XML.
     *
     * @param node         узел GPathResult
     * @param propertyName имя дочернего элемента
     * @return текстовое содержимое элемента или пустая строка, если элемент отсутствует
     */
    private String getChildText(GPathResult node, String propertyName) {
        try {
            Object value = node.getProperty(propertyName);
            if (value instanceof GPathResult) {
                String text = ((GPathResult) value).text();
                return text != null ? text : "";
            }
            return value != null ? value.toString() : "";
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Преобразует строку в BigDecimal. Заменяет запятую на точку.
     *
     * @param value строковое представление числа
     * @return BigDecimal, или {@code BigDecimal.ZERO} при ошибке парсинга
     */
    private BigDecimal parseBigDecimal(String value) {
        if (value == null || value.trim().isEmpty()) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(value.replace(",", ".").trim());
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }

    /**
     * Преобразует строку в Integer.
     *
     * @param value        строковое представление целого числа
     * @param defaultValue значение по умолчанию, если строка пуста или не может быть преобразована
     * @return Integer или defaultValue
     */
    private Integer parseInt(String value, Integer defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}