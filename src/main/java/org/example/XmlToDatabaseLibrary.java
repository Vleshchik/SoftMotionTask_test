package org.example;

import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.GPathResult;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
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

    public XmlToDatabaseLibrary(String xmlUrl, String dbUrl, String dbUser, String dbPassword) {
        this.xmlUrl = xmlUrl;
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    public XmlToDatabaseLibrary() {
        this("https://expro.ru/bitrix/catalog_export/export_Sai.xml",
                "jdbc:postgresql://localhost:5432/expro_db",
                "user",
                "password");
    }

    /**
     * Получает корневой элемент XML
     */
    private GPathResult getXmlRoot() throws IOException, SAXException, ParserConfigurationException {
        try (InputStream is = new URL(xmlUrl).openStream()) {
            return new XmlSlurper().parse(is);
        }
    }

    /**
     * 1. Возвращает названия основных таблиц из XML.
     */
    public List<String> getTableNames() {
        return Arrays.asList("currency", "categories", "offers");
    }

    /**
     * 2. Создает SQL DDL для таблицы на основе анализа XML.
     */
    public String getTableDDL(String tableName) {
        try {
            switch (tableName.toLowerCase()) {
                case "currency":
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

    private String generateCurrencyDDL() {
        return "CREATE TABLE IF NOT EXISTS currency (\n" +
                "    id SERIAL PRIMARY KEY,\n" +
                "    currency_id VARCHAR(10) UNIQUE,\n" +
                "    code VARCHAR(10),\n" +
                "    name TEXT,\n" +
                "    rate NUMERIC\n" +
                ");";
    }

    private String generateCategoriesDDL() {
        return "CREATE TABLE IF NOT EXISTS categories (\n" +
                "    id SERIAL PRIMARY KEY,\n" +
                "    category_id INTEGER UNIQUE,\n" +
                "    name TEXT,\n" +
                "    parent_id INTEGER\n" +
                ");";
    }

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
     * 3. Обновляет все таблицы.
     */
    public void update() throws SQLException {
        for (String tableName : getTableNames()) {
            update(tableName);
        }
    }

    /**
     * 4. Обновляет конкретную таблицу.
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
                    case "currency":
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

    @SuppressWarnings("unchecked")
    private void updateCurrency(Connection conn, GPathResult root) throws SQLException {
        // Создаем таблицу если её нет
        createTableIfNotExists(conn, "currency", generateCurrencyDDL());

        String upsertSql = "INSERT INTO currency (currency_id, code, name, rate) VALUES (?, ?, ?, ?) " +
                "ON CONFLICT (currency_id) DO UPDATE SET code = EXCLUDED.code, " +
                "name = EXCLUDED.name, rate = EXCLUDED.rate";

        try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
            Object currenciesObj = root.getProperty("currency");
            if (currenciesObj instanceof GPathResult) {
                GPathResult currencies = (GPathResult) currenciesObj;
                Iterator<Object> iterator = currencies.iterator();
                while (iterator.hasNext()) {
                    Object obj = iterator.next();
                    if (obj instanceof GPathResult) {
                        GPathResult currency = (GPathResult) obj;

                        String id = getAttribute(currency, "id");
                        String code = getAttribute(currency, "code");
                        String name = getChildText(currency, "name");
                        String rate = getChildText(currency, "rate");

                        ps.setString(1, id);
                        ps.setString(2, code);
                        ps.setString(3, name);
                        ps.setBigDecimal(4, parseBigDecimal(rate));
                        ps.addBatch();
                    }
                }
            }
            ps.executeBatch();
        }
    }

    @SuppressWarnings("unchecked")
    private void updateCategories(Connection conn, GPathResult root) throws SQLException {
        // Создаем таблицу если её нет
        createTableIfNotExists(conn, "categories", generateCategoriesDDL());

        String upsertSql = "INSERT INTO categories (category_id, name, parent_id) VALUES (?, ?, ?) " +
                "ON CONFLICT (category_id) DO UPDATE SET name = EXCLUDED.name, " +
                "parent_id = EXCLUDED.parent_id";

        try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
            Object categoriesObj = root.getProperty("categories");
            if (categoriesObj instanceof GPathResult) {
                GPathResult categories = (GPathResult) categoriesObj;
                Object categoryListObj = categories.getProperty("category");

                if (categoryListObj instanceof GPathResult) {
                    GPathResult categoryList = (GPathResult) categoryListObj;
                    Iterator<Object> iterator = categoryList.iterator();

                    while (iterator.hasNext()) {
                        Object obj = iterator.next();
                        if (obj instanceof GPathResult) {
                            GPathResult category = (GPathResult) obj;

                            Integer id = parseInt(getAttribute(category, "id"), null);
                            String name = getChildText(category, "name");
                            Integer parentId = parseInt(getAttribute(category, "parentId"), null);

                            if (id != null) {
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
                }
            }
            ps.executeBatch();
        }
    }

    @SuppressWarnings("unchecked")
    private void updateOffers(Connection conn, GPathResult root) throws SQLException {
        // Создаем таблицы если их нет
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

            Object offersObj = root.getProperty("offers");
            if (offersObj instanceof GPathResult) {
                GPathResult offers = (GPathResult) offersObj;
                Object offerListObj = offers.getProperty("offer");

                if (offerListObj instanceof GPathResult) {
                    GPathResult offerList = (GPathResult) offerListObj;
                    Iterator<Object> iterator = offerList.iterator();

                    while (iterator.hasNext()) {
                        Object obj = iterator.next();
                        if (obj instanceof GPathResult) {
                            GPathResult offer = (GPathResult) obj;

                            String offerId = getAttribute(offer, "id");
                            String vendorCode = getChildText(offer, "vendorCode");
                            String available = getAttribute(offer, "available");
                            String name = getChildText(offer, "name");
                            String price = getChildText(offer, "price");
                            String currencyId = getChildText(offer, "currencyId");
                            String categoryId = getChildText(offer, "categoryId");
                            String picture = getChildText(offer, "picture");
                            String description = getChildText(offer, "description");

                            if (vendorCode == null || vendorCode.isEmpty()) {
                                continue;
                            }

                            // 1. UPSERT offer
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

                            // 2. Удаляем старые атрибуты
                            psDeleteAttr.setString(1, vendorCode);
                            psDeleteAttr.addBatch();

                            // 3. Вставляем новые атрибуты
                            Object paramsObj = offer.getProperty("param");
                            if (paramsObj instanceof GPathResult) {
                                GPathResult params = (GPathResult) paramsObj;
                                Iterator<Object> paramIterator = params.iterator();

                                while (paramIterator.hasNext()) {
                                    Object paramObj = paramIterator.next();
                                    if (paramObj instanceof GPathResult) {
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
                }
            }

            psOffer.executeBatch();
            psDeleteAttr.executeBatch();
            psInsertAttr.executeBatch();
        }
    }

    /**
     * Создает таблицу если она не существует
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
     * Получает список колонок таблицы.
     */
    public List<String> getColumnNames(String tableName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            return new ArrayList<>(getTableColumns(conn, tableName));
        }
    }

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
     * Проверяет, является ли колонка уникальным идентификатором.
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
     * Генерирует SQL для добавления новых колонок.
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

    private Set<String> getExpectedColumns(String tableName) {
        Set<String> columns = new HashSet<>();
        switch (tableName.toLowerCase()) {
            case "currency":
                columns.addAll(Arrays.asList("id", "currency_id", "code", "name", "rate"));
                break;
            case "categories":
                columns.addAll(Arrays.asList("id", "category_id", "name", "parent_id"));
                break;
        }
        return columns;
    }

    // Вспомогательные методы для работы с XML
    private String getAttribute(GPathResult node, String name) {
        try {
            Object value = node.getProperty("@" + name);
            return value != null ? value.toString() : "";
        } catch (Exception e) {
            return "";
        }
    }

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