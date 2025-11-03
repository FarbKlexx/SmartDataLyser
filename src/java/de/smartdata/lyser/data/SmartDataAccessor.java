package de.smartdata.lyser.data;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.scl.rest.util.WebTargetCreator;
import de.smartdata.lyser.config.Configuration;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonParser;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.StringReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Map;

/**
 * Methods for accessing a SmartData instance to get data. Aims to simplify the
 * access of SmartData from other Java programs
 *
 * @author Florian Fehring
 */
public class SmartDataAccessor {

    protected String jndi = null;
    protected DataSource ds = null;
    protected String smartdataRequest; // Contains last called URL

    /**
     * Geneal purpose accessor
     */
    public SmartDataAccessor() {

    }

    /**
     * Optimized SmartDataAccessor for a samrtdata instance
     *
     * @param smartdataurl URL to SmartData instance
     */
    public SmartDataAccessor(String smartdataurl) {
        // Get SmartData instance name
        int lastSlash = smartdataurl.lastIndexOf("/");
        String smartdataname = smartdataurl;
        if (lastSlash >= 0) {
            smartdataname = smartdataurl.substring(lastSlash + 1);
        }
        // Load configuration for instance
        Configuration conf = new Configuration(smartdataname);
        this.jndi = conf.getProperty("postgres.jndi");
        if (this.jndi == null) {
            this.jndi = "jdbc/SmartData";
        }
        try {
            InitialContext ctx = new InitialContext();
            this.ds = (DataSource) ctx.lookup(this.jndi);
        } catch (NamingException ex) {
            Message msg = new Message("", MessageLevel.ERROR, "Could not access connection pool: " + ex.getLocalizedMessage());
            Logger.addMessage(msg);
        }
    }

    public String getJndi() {
        return this.jndi;
    }

    public Connection getConnection() {
        if (this.ds == null) {
            return null;
        }
        try {
            return this.ds.getConnection();
        } catch (SQLException ex) {
            Message msg = new Message("", MessageLevel.ERROR, "Could not conntect to database: " + ex.getLocalizedMessage());
            Logger.addMessage(msg);
        }
        return null;
    }

    /**
     * Gets the number of available datasets
     *
     * @param smartdataurl SmartDatas URL
     * @param collection Collections name
     * @param storage Storages name
     * @param exact true if exact value is wanted
     *
     * @return Number of available datasets
     * @throws de.smartdata.lyser.data.SmartDataAccessorException
     */
    public int fetchCount(String smartdataurl, String collection, String storage, boolean exact) throws SmartDataAccessorException {
        return this.fetchCount(smartdataurl, collection, storage, null, null, null, exact);
    }

    /**
     * Gets the number of available datasets
     *
     * @param smartdataurl SmartDatas URL
     * @param collection Collections name
     * @param storage Storages name
     * @param dateattr Date values holding attribute name
     * @param start Startdate
     * @param end Enddate
     * @param exact true if exact value is wanted
     *
     * @return Number of available datasets
     * @throws de.smartdata.lyser.data.SmartDataAccessorException
     */
    public int fetchCount(String smartdataurl, String collection, String storage, String dateattr, LocalDateTime start, LocalDateTime end, boolean exact) throws SmartDataAccessorException {

        // Local direct db access
        Connection con = this.getConnection();
        if (con != null) {
            try {
                // SQL-Abfrage mit einem Platzhalter für die Tabelle
                String sql = "SELECT reltuples AS estimate FROM pg_class WHERE relname = '" + collection + "'";
                if (exact) {
                    sql = "SELECT COUNT(*) FROM \"" + storage + "\".\"" + collection + "\"";
                }
                PreparedStatement preparedStatement = con.prepareStatement(sql);

                // Abfrage ausführen
                ResultSet resultSet = preparedStatement.executeQuery();

                // Ergebnis verarbeiten
                int count = 0;
                if (resultSet.next()) {
                    count = resultSet.getInt(1);
                }
                resultSet.close();
                preparedStatement.close();
                return count;
            } catch (Exception ex) {
                throw new SmartDataAccessorException("Could not get data from >" + collection + "< an sql error occured: " + ex.getLocalizedMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new SmartDataAccessorException("Could not close db connection. Possible memory leak." + ex.getLocalizedMessage());
                }
            }
        }

        // Get information about file from SmartData
        WebTarget webTarget = WebTargetCreator.createWebTarget(
                smartdataurl + "/smartdata", "records")
                .path(collection)
                .queryParam("storage", storage)
                .queryParam("countonly", true);
        if (start != null && end != null) {
            webTarget = webTarget.queryParam("filter", dateattr + ",gt," + start);
            webTarget = webTarget.queryParam("filter", dateattr + ",lt," + end);
        }

        // Note request URI for documentation
        this.smartdataRequest = webTarget.getUri().toString();

        Response response = webTarget.request(MediaType.APPLICATION_JSON).get();
        String responseText = response.readEntity(String.class);
        if (Response.Status.OK.getStatusCode() == response.getStatus()) {
            JsonParser parser = Json.createParser(new StringReader(responseText));
            parser.next();
            JsonArray records = parser.getObject().getJsonArray("records");
            if (records == null) {
                throw new SmartDataAccessorException("Could not get data from >" + webTarget.getUri() + "< retuned no >records<");
            }
            JsonObject cobj = records.getJsonObject(0);
            if (cobj == null) {
                throw new SmartDataAccessorException("Could not get data from >" + webTarget.getUri() + "< retuned no >data<");
            }
            return cobj.getInt("count");
        }
        throw new SmartDataAccessorException("Could not access >" + webTarget.getUri() + "< returned status: " + response.getStatus());
    }

    /**
     * Calculates the aritmethic mean from a column
     *
     * @param smartdataurl URL of smartdata (e.g.
     * http://localhost:8080/SmartData)
     * @param collection Collections name (Tablename)
     * @param storage Storage name (Schemaname)
     * @param dateattr Name of the attribute that holds date information
     * @param start Start date of datasets used for calculation
     * @param end End date of datasets used for calculation
     * @param column Name of column to calculate the median
     * @return Arithmetic mean value
     * @throws SmartDataAccessorException
     */
    public double fetchArithmeticMean(String smartdataurl, String collection, String storage, String dateattr, LocalDateTime start, LocalDateTime end, String column) throws SmartDataAccessorException {
        // If available use local direct db access
        Connection con = this.getConnection();
        if (con != null) {
            try {
                // SQL for arithmetic mean
                String sql = "SELECT AVG(" + column + ") AS average FROM \"" + storage + "\".\"" + collection + "\"";
                if (dateattr != null && start != null && end != null) {
                    sql += " WHERE " + dateattr + " >= '" + start + "' AND " + dateattr + " <= '" + end + "'";
                }
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery();
                double average = 0.0;
                if (resultSet.next()) {
                    average = resultSet.getDouble(1);
                }
                resultSet.close();
                preparedStatement.close();
                return average;
            } catch (Exception ex) {
                throw new SmartDataAccessorException("Could not get data from >" + collection + "< an SQL error occurred: " + ex.getLocalizedMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new SmartDataAccessorException("Could not close DB connection. Possible memory leak." + ex.getLocalizedMessage());
                }
            }
        }

        // Use SmartData API and calculate arithmetic mean
        List<Double> values = new ArrayList<>();

        String includes = column;
        String order = column + ",DESC";
        // Get datasets
        JsonArray datasets = this.fetchData(smartdataurl, collection, storage, includes, null, dateattr, start, end, order);
        for (JsonNumber curVal : datasets.getValuesAs(JsonNumber.class)) {
            values.add(curVal.bigDecimalValue().doubleValue());
        }

        double sum = 0.0;
        for (double value : values) {
            sum += value;
        }
        double average = values.isEmpty() ? 0.0 : sum / values.size();

        return average;
    }

    /**
     * Calculates the standard deviation from a column
     *
     * @param smartdataurl URL of smartdata (e.g.
     * http://localhost:8080/SmartData)
     * @param collection Collections name (Tablename)
     * @param storage Storage name (Schemaname)
     * @param dateattr Name of the attribute that holds date information
     * @param start Start date of datasets used for calculation
     * @param end End date of datasets used for calculation
     * @param column Name of column to calculate the median
     * @return Standard deviation value
     * @throws SmartDataAccessorException
     */
    public double fetchStdDviation(String smartdataurl, String collection, String storage, String dateattr, LocalDateTime start, LocalDateTime end, String column) throws SmartDataAccessorException {
        // If available use local direct db access
        Connection con = this.getConnection();
        if (con != null) {
            try {
                // SQL for standard deviation
                String sql = "SELECT STDDEV(" + column + ") AS stddev FROM \"" + storage + "\".\"" + collection + "\"";
                if (dateattr != null && start != null && end != null) {
                    sql += " WHERE " + dateattr + " >= '" + start + "' AND " + dateattr + " <= '" + end + "'";
                }
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery();

                double stddev = 0.0;
                if (resultSet.next()) {
                    stddev = resultSet.getDouble(1);
                }
                resultSet.close();
                preparedStatement.close();
                return stddev;
            } catch (Exception ex) {
                throw new SmartDataAccessorException("Could not get data from >" + collection + "< an SQL error occurred: " + ex.getLocalizedMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new SmartDataAccessorException("Could not close DB connection. Possible memory leak." + ex.getLocalizedMessage());
                }
            }
        }

        // use SmartData API
        List<Double> values = new ArrayList<>();

        String includes = column;
        String order = column + ",DESC";
        // Get datasets
        JsonArray datasets = this.fetchData(smartdataurl, collection, storage, includes, null, dateattr, start, end, order);
        for (JsonNumber curVal : datasets.getValuesAs(JsonNumber.class)) {
            values.add(curVal.bigDecimalValue().doubleValue());
        }

        // Calculate standard deviation
        double sum = 0.0;
        for (double value : values) {
            sum += value;
        }
        double mean = values.isEmpty() ? 0.0 : sum / values.size();

        double varianceSum = 0.0;
        for (double value : values) {
            varianceSum += Math.pow(value - mean, 2);
        }
        double standardDeviation = values.isEmpty() ? 0.0 : Math.sqrt(varianceSum / values.size());

        return standardDeviation;
    }

    /**
     * Calculates the median from a column
     *
     * @param smartdataurl URL of smartdata (e.g.
     * http://localhost:8080/SmartData)
     * @param collection Collections name (Tablename)
     * @param storage Storage name (Schemaname)
     * @param dateattr Name of the attribute that holds date information
     * @param start Start date of datasets used for calculation
     * @param end End date of datasets used for calculation
     * @param column Name of column to calculate the median
     * @return Median value
     * @throws SmartDataAccessorException
     */
    public double fetchMedian(String smartdataurl, String collection, String storage, String dateattr, LocalDateTime start, LocalDateTime end, String column) throws SmartDataAccessorException {
        // If avaiable use local direct db access
        Connection con = this.getConnection();
        if (con != null) {
            try {
                // SQL-Abfrage mit einem Platzhalter für die Tabelle
                String sql = "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY '" + column + "') AS median FROM \"" + storage + "\".\"" + collection + "\"";
                if (dateattr != null && start != null && end != null) {
                    sql += " WHERE " + dateattr + " >= '" + start + "' AND " + dateattr + " <= '" + end + "'";
                }
                PreparedStatement preparedStatement = con.prepareStatement(sql);

                // Abfrage ausführen
                ResultSet resultSet = preparedStatement.executeQuery();

                // Ergebnis verarbeiten
                int count = 0;
                if (resultSet.next()) {
                    count = resultSet.getInt(1);
                }
                resultSet.close();
                preparedStatement.close();
                return count;
            } catch (Exception ex) {
                throw new SmartDataAccessorException("Could not get data from >" + collection + "< an sql error occured: " + ex.getLocalizedMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new SmartDataAccessorException("Could not close db connection. Possible memory leak." + ex.getLocalizedMessage());
                }
            }
        }

        // Folowing code uses the SmartData API for getting the data and calculates the median itself.
        List<Double> values = new ArrayList<>();

        String includes = column;
        String order = column + ",DESC";
        // Get datasets
        JsonArray datasets = this.fetchData(smartdataurl, collection, storage, includes, null, dateattr, start, end, order);
        for (JsonNumber curVal : datasets.getValuesAs(JsonNumber.class)) {
            values.add(curVal.bigDecimalValue().doubleValue());
        }
        // Median berechnen
        double median;
        int size = values.size();
        if (size % 2 == 0) {
            // Durchschnitt der zwei mittleren Werte bei gerader Anzahl
            median = (values.get(size / 2 - 1) + values.get(size / 2)) / 2.0;
        } else {
            // Mittlerer Wert bei ungerader Anzahl
            median = values.get(size / 2);
        }
        return median;
    }

    /**
     * Calculates the minimum from a column
     *
     * @param smartdataurl URL of smartdata (e.g.
     * http://localhost:8080/SmartData)
     * @param collection Collections name (Tablename)
     * @param storage Storage name (Schemaname)
     * @param dateattr Name of the attribute that holds date information
     * @param start Start date of datasets used for calculation
     * @param end End date of datasets used for calculation
     * @param column Name of column to calculate the median
     * @return Minimum value
     * @throws SmartDataAccessorException
     */
    public double fetchMin(String smartdataurl, String collection, String storage, String dateattr, LocalDateTime start, LocalDateTime end, String column) throws SmartDataAccessorException {

        // Local direct db access
        Connection con = this.getConnection();
        if (con != null) {
            try {
                // SQL-Abfrage mit einem Platzhalter für die Tabelle
                String sql = "SELECT MIN(" + column + ") FROM \"" + storage + "\".\"" + collection + "\"";
                PreparedStatement preparedStatement = con.prepareStatement(sql);

                // Abfrage ausführen
                ResultSet resultSet = preparedStatement.executeQuery();

                // Ergebnis verarbeiten
                int count = 0;
                if (resultSet.next()) {
                    count = resultSet.getInt(1);
                }
                resultSet.close();
                preparedStatement.close();
                return count;
            } catch (Exception ex) {
                throw new SmartDataAccessorException("Could not get data from >" + collection + "< an sql error occured: " + ex.getLocalizedMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new SmartDataAccessorException("Could not close db connection. Possible memory leak." + ex.getLocalizedMessage());
                }
            }
        }

        // Get information about file from SmartData
        WebTarget webTarget = WebTargetCreator.createWebTarget(
                smartdataurl + "/smartdata", "records")
                .path(collection)
                .queryParam("storage", storage)
                .queryParam("oder", column + ",DESC")
                .queryParam("size", 1);
        if (start != null && end != null) {
            webTarget = webTarget.queryParam("filter", dateattr + ",gt," + start);
            webTarget = webTarget.queryParam("filter", dateattr + ",lt," + end);
        }

        // Note request URI for documentation
        this.smartdataRequest = webTarget.getUri().toString();

        Response response = webTarget.request(MediaType.APPLICATION_JSON).get();
        String responseText = response.readEntity(String.class);
        if (Response.Status.OK.getStatusCode() == response.getStatus()) {
            JsonParser parser = Json.createParser(new StringReader(responseText));
            parser.next();
            JsonArray records = parser.getObject().getJsonArray("records");
            if (records == null) {
                throw new SmartDataAccessorException("Could not get data from >" + webTarget.getUri() + "< retuned no >records<");
            }
            JsonObject cobj = records.getJsonObject(0);
            if (cobj == null) {
                throw new SmartDataAccessorException("Could not get data from >" + webTarget.getUri() + "< retuned no >data<");
            }
            return cobj.getInt("count");
        }
        throw new SmartDataAccessorException("Could not access >" + webTarget.getUri() + "< returned status: " + response.getStatus());

    }

    /**
     * Calculates the maximum from a column
     *
     * @param smartdataurl URL of smartdata (e.g.
     * http://localhost:8080/SmartData)
     * @param collection Collections name (Tablename)
     * @param storage Storage name (Schemaname)
     * @param dateattr Name of the attribute that holds date information
     * @param start Start date of datasets used for calculation
     * @param end End date of datasets used for calculation
     * @param column Name of column to calculate the median
     * @return Maximum value
     * @throws SmartDataAccessorException
     */
    public double fetchMax(String smartdataurl, String collection, String storage, String dateattr, LocalDateTime start, LocalDateTime end, String column) throws SmartDataAccessorException {

        // Local direct db access
        Connection con = this.getConnection();
        if (con != null) {
            try {
                // SQL-Abfrage mit einem Platzhalter für die Tabelle
                String sql = "SELECT MAX(" + column + ") FROM \"" + storage + "\".\"" + collection + "\"";
                PreparedStatement preparedStatement = con.prepareStatement(sql);

                // Abfrage ausführen
                ResultSet resultSet = preparedStatement.executeQuery();

                // Ergebnis verarbeiten
                int count = 0;
                if (resultSet.next()) {
                    count = resultSet.getInt(1);
                }
                resultSet.close();
                return count;
            } catch (Exception ex) {
                throw new SmartDataAccessorException("Could not get data from >" + collection + "< an sql error occured: " + ex.getLocalizedMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new SmartDataAccessorException("Could not close db connection. Possible memory leak." + ex.getLocalizedMessage());
                }
            }
        }

        // Get information about file from SmartData
        WebTarget webTarget = WebTargetCreator.createWebTarget(
                smartdataurl + "/smartdata", "records")
                .path(collection)
                .queryParam("storage", storage)
                .queryParam("oder", column + ",ASC")
                .queryParam("size", 1);
        if (start != null && end != null) {
            webTarget = webTarget.queryParam("filter", dateattr + ",gt," + start);
            webTarget = webTarget.queryParam("filter", dateattr + ",lt," + end);
        }

        // Note request URI for documentation
        this.smartdataRequest = webTarget.getUri().toString();

        Response response = webTarget.request(MediaType.APPLICATION_JSON).get();
        String responseText = response.readEntity(String.class);
        if (Response.Status.OK.getStatusCode() == response.getStatus()) {
            JsonParser parser = Json.createParser(new StringReader(responseText));
            parser.next();
            JsonArray records = parser.getObject().getJsonArray("records");
            if (records == null) {
                throw new SmartDataAccessorException("Could not get data from >" + webTarget.getUri() + "< retuned no >records<");
            }
            JsonObject cobj = records.getJsonObject(0);
            if (cobj == null) {
                throw new SmartDataAccessorException("Could not get data from >" + webTarget.getUri() + "< retuned no >data<");
            }
            return cobj.getInt("count");
        }
        throw new SmartDataAccessorException("Could not access >" + webTarget.getUri() + "< returned status: " + response.getStatus());

    }

    /**
     * Get data from the SmartData and return it as JSON
     *
     * @param smartdataurl SmartDatas URL
     * @param collection Collections name
     * @param storage Storages name
     * @param includes List of attributes that should be returned
     * @param filters Any filter statement accepted by SmartData
     * @param dateattr Attribute that stores date information (if start and end
     * should be used)
     * @param start Startdate to look at
     * @param end Enddate to look at
     * @param order Attribute name to order by
     * @return JSON with available data
     * @throws de.smartdata.lyser.data.SmartDataAccessorException
     */
    public JsonArray fetchData(String smartdataurl, String collection, String storage, String includes, List<String> filters, String dateattr, LocalDateTime start, LocalDateTime end, String order) throws SmartDataAccessorException {
        Integer limit = null;
        // Local direct db access
        Connection con = this.getConnection();
        if (con != null && filters == null) {
            if (includes == null) {
                includes = "*";
            }
            try {
                // SQL-Abfrage mit einem Platzhalter für die Tabelle
                String sql = "SELECT " + includes + " FROM \"" + storage + "\".\"" + collection + "\"";
                if (dateattr != null && start != null && end != null) {
                    sql += " WHERE " + dateattr + " >= '" + start + "' AND " + dateattr + " <= '" + end + "'";
                }
                if (order != null) {
                    sql += " ORDER BY " + order.replace(',', ' ');
                }
                if (limit != null) {
                    sql += " LIMIT " + limit;
                }
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                // Abfrage ausführen
                ResultSet resultSet = preparedStatement.executeQuery();

                // Ergebnis in eine Liste von Maps umwandeln
                JsonArrayBuilder newdataarr = Json.createArrayBuilder();
                while (resultSet.next()) {
                    JsonObjectBuilder newdataset = Json.createObjectBuilder();
                    for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                        String colName = resultSet.getMetaData().getColumnName(i);
                        String colType = resultSet.getMetaData().getColumnTypeName(i);

                        switch (colType) {
                            case "bool":
                            case "boolean":
                                newdataset.add(colName, resultSet.getBoolean(i));
                                break;
                            case "int":
                            case "int4":
                                newdataset.add(colName, resultSet.getInt(i));
                                break;
                            case "int8":
                            case "bigserial":
                                newdataset.add(colName, resultSet.getLong(i));
                                break;
                            case "float":
                            case "float4":
                                newdataset.add(colName, resultSet.getFloat(i));
                                break;
                            case "float8":
                                newdataset.add(colName, resultSet.getDouble(i));
                                break;
                            case "timestamp":
                                Date timestamp = resultSet.getTimestamp(i);
                                if (timestamp != null) {
                                    newdataset.add(colName, timestamp.toString());
                                } else {
                                    System.out.println("TEST timestamp column >" + colName + "< has value: " + timestamp);
                                }
                                break;
                            case "date":
                                Date date = resultSet.getDate(i);
                                newdataset.add(colName, date.toString());
                                break;
                            case "varchar":
                                newdataset.add(colName, resultSet.getString(i));
                                break;
                            default:
                                System.out.println("Unsupported column type >" + colType + "< used.");
                        }
                    }
                    newdataarr.add(newdataset);
                }
                resultSet.close();
                preparedStatement.close();
                String json = newdataarr.build().toString();
                try (JsonReader reader = Json.createReader(new StringReader(json))) {
                    // Lese das JSON-Array
                    JsonArray records = reader.readArray();
                    return records;
                } catch (Exception e) {
                    System.err.println("Fehler beim Parsen des JSON-Strings: " + e.getMessage());
                }
            } catch (Exception ex) {
                throw new SmartDataAccessorException("Could not get data from >" + collection + "< an sql error occured: " + ex.getLocalizedMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new SmartDataAccessorException("Could not close db connection. Possible memory leak." + ex.getLocalizedMessage());
                }
            }
        } else if (con != null) {
            try {
                con.close();
            } catch (SQLException ex) {
                throw new SmartDataAccessorException("Could not close db connection. Possible memory leak." + ex.getLocalizedMessage());
            }
        }

        // Get information about file from SmartData
        WebTarget webTarget = WebTargetCreator.createWebTarget(smartdataurl + "/smartdata", "records")
                .path(collection)
                .queryParam("storage", storage);
        if (filters != null) {
            for(String curFilter : filters) {
                if(curFilter.contains("&filter=")) {
                    String[] subFilters = curFilter.split("&filter=");
                    for(String curSubFilter : subFilters) {
                        webTarget = webTarget.queryParam("filter", curSubFilter);
                    }
                } else {
                    webTarget = webTarget.queryParam("filter", curFilter);
                }
            }
        }
        if (includes != null) {
            webTarget = webTarget.queryParam("includes", includes);
        }
        if (start != null) {
            webTarget = webTarget.queryParam("filter", dateattr + ",gt," + start);
        }
        if (end != null) {
            webTarget = webTarget.queryParam("filter", dateattr + ",lt," + end);
        }
        if (order != null) {
            webTarget = webTarget.queryParam("order", order.replace(" ", ","));
        }
        if (limit != null) {
            webTarget = webTarget.queryParam("size", limit);
        }

        // Note request URI for documentation
        this.smartdataRequest = webTarget.getUri().toString();

        Response response = webTarget.request(MediaType.APPLICATION_JSON).get();
        String responseText = response.readEntity(String.class);
        if (Response.Status.OK.getStatusCode() == response.getStatus()) {
            JsonParser parser = Json.createParser(new StringReader(responseText));
            parser.next();
            JsonArray records = parser.getObject().getJsonArray("records");
            if (records == null) {
                throw new SmartDataAccessorException("Could not get data from >" + webTarget.getUri() + "< retuned no >records<");
            }
            return records;
        } else {
            throw new SmartDataAccessorException("Could not access >" + webTarget.getUri() + "< returned status: " + response.getStatus());
        }
    }
    
    /**
     * Get data from the SmartData and return it as JSON
     *
     * @param smartdataurl SmartDatas URL
     * @param collection Collections name
     * @param storage Storages name
     * @param includes List of attributes that should be returned
     * @param filters Any filter statement accepted by SmartData
     * @param dateattr Attribute that stores date information (if start and end
     * should be used)
     * @param start Startdate to look at
     * @param end Enddate to look at
     * @param order Attribute name to order by
     * @return JSON with available data
     * @throws de.smartdata.lyser.data.SmartDataAccessorException
     */
    public JsonArray fetchDataSupNull(String smartdataurl, String collection, String storage, String includes, List<String> filters, String dateattr, LocalDateTime start, LocalDateTime end, String order) throws SmartDataAccessorException {
        Integer limit = null;
        // Local direct db access
        Connection con = this.getConnection();
        if (con != null && filters == null) {
            if (includes == null) {
                includes = "*";
            }
            try {
                // SQL-Abfrage mit einem Platzhalter für die Tabelle
                String sql = "SELECT " + includes + " FROM \"" + storage + "\".\"" + collection + "\"";
                if (dateattr != null && start != null && end != null) {
                    sql += " WHERE " + dateattr + " >= '" + start + "' AND " + dateattr + " <= '" + end + "'";
                }
                if (order != null) {
                    sql += " ORDER BY " + order.replace(',', ' ');
                }
                if (limit != null) {
                    sql += " LIMIT " + limit;
                }
                JsonArrayBuilder newdataarr;
                // Abfrage ausführen
                try (PreparedStatement preparedStatement = con.prepareStatement(sql); // Abfrage ausführen
                        ResultSet resultSet = preparedStatement.executeQuery()) {
                    // Ergebnis in eine Liste von Maps umwandeln
                    newdataarr = Json.createArrayBuilder();
                    while (resultSet.next()) {
                        JsonObjectBuilder newdataset = Json.createObjectBuilder();
                        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                            String colName = resultSet.getMetaData().getColumnName(i);
                            String colType = resultSet.getMetaData().getColumnTypeName(i);
                            
                            switch (colType) {
                                case "bool", "boolean" -> {
                                    boolean boolValue = resultSet.getBoolean(i);
                                    if (resultSet.wasNull()) {
                                        newdataset.addNull(colName);
                                    } else {
                                        newdataset.add(colName, boolValue);
                                    }
                                }
                                case "int", "int4" -> {
                                    int intValue = resultSet.getInt(i);
                                    if (resultSet.wasNull()) {
                                        newdataset.addNull(colName);
                                    } else {
                                        newdataset.add(colName, intValue);
                                    }
                                }
                                case "int8", "bigserial" -> {
                                    long longValue = resultSet.getLong(i);
                                    if (resultSet.wasNull()) {
                                        newdataset.addNull(colName);
                                    } else {
                                        newdataset.add(colName, longValue);
                                    }
                                }
                                case "float", "float4" -> {
                                    float floatValue = resultSet.getFloat(i);
                                    if (resultSet.wasNull()) {
                                        newdataset.addNull(colName);
                                    } else {
                                        newdataset.add(colName, floatValue);
                                    }
                                }
                                case "float8" -> {
                                    double doubleValue = resultSet.getDouble(i);
                                    if (resultSet.wasNull()) {
                                        newdataset.addNull(colName);
                                    } else {
                                        newdataset.add(colName, doubleValue);
                                    }
                                }
                                case "timestamp" -> {
                                    Timestamp timestamp = resultSet.getTimestamp(i);
                                    if (resultSet.wasNull()) {
                                        newdataset.addNull(colName);
                                    } else {
                                        newdataset.add(colName, timestamp.toString());
                                    }
                                }
                                case "date" -> {
                                    Date date = resultSet.getDate(i);
                                    if (resultSet.wasNull()) {
                                        newdataset.addNull(colName);
                                    } else {
                                        newdataset.add(colName, date.toString());
                                    }
                                }
                                case "varchar", "text", "jsonb", "json", "uuid" -> {
                                    String stringValue = resultSet.getString(i);
                                    if (resultSet.wasNull()) {
                                        newdataset.addNull(colName);
                                    } else {
                                        newdataset.add(colName, stringValue);
                                    }
                                }
                                default -> {
                                    String stringValue = resultSet.getString(i);
                                    if (resultSet.wasNull()) {
                                        newdataset.addNull(colName);
                                    } else {
                                        newdataset.add(colName, stringValue);
                                    }
                                }
                            }

                           
                        }
                        newdataarr.add(newdataset);
                    }                  }
                String json = newdataarr.build().toString();
                try (JsonReader reader = Json.createReader(new StringReader(json))) {
                    // Lese das JSON-Array
                    JsonArray records = reader.readArray();
                    return records;
                } catch (Exception e) {
                    System.err.println("Fehler beim Parsen des JSON-Strings: " + e.getMessage());
                }
            } catch (SQLException ex) {
                throw new SmartDataAccessorException("Could not get data from >" + collection + "< an sql error occured: " + ex.getLocalizedMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new SmartDataAccessorException("Could not close db connection. Possible memory leak." + ex.getLocalizedMessage());
                }
            }
        } else if (con != null) {
            try {
                con.close();
            } catch (SQLException ex) {
                throw new SmartDataAccessorException("Could not close db connection. Possible memory leak." + ex.getLocalizedMessage());
            }
        }
        // ... lokaler Teil oben bleibt wie bei dir

        // Standard setzen, bevor das WebTarget gebaut wird
        if (includes == null || includes.isBlank()) {
            includes = "*";
        }

        // WebTarget bauen
        WebTarget webTarget = WebTargetCreator.createWebTarget(smartdataurl + "/smartdata", "records")
                .path(collection)
                .queryParam("storage", storage)
                .queryParam("includes", includes); // reicht – nicht nochmal unten anhängen

        // Filter anhängen
        if (filters != null && !filters.isEmpty()) {
            for (String curFilter : filters) {
                if (curFilter.contains("&filter=")) {
                    String[] subFilters = curFilter.split("&filter=");
                    for (String curSubFilter : subFilters) {
                        if (!curSubFilter.isBlank())
                            webTarget = webTarget.queryParam("filter", curSubFilter);
                    }
                } else {
                    webTarget = webTarget.queryParam("filter", curFilter);
                }
            }
        }

        if (start != null && dateattr != null) {
            webTarget = webTarget.queryParam("filter", dateattr + ",gt," + start); // ggf. ISO-Format verwenden
        }
        if (end != null && dateattr != null) {
            webTarget = webTarget.queryParam("filter", dateattr + ",lt," + end);
        }
        if (order != null) {
            webTarget = webTarget.queryParam("order", order.replace(" ", ",")); // wie gehabt
        }
        if (limit != null) {
            webTarget = webTarget.queryParam("size", limit);
        }


        // Note request URI for documentation
        this.smartdataRequest = webTarget.getUri().toString();

        Response response = webTarget.request(MediaType.APPLICATION_JSON).get();
        String responseText = response.readEntity(String.class);
        if (Response.Status.OK.getStatusCode() == response.getStatus()) {
            JsonParser parser = Json.createParser(new StringReader(responseText));
            parser.next();
            JsonArray records = parser.getObject().getJsonArray("records");
            if (records == null) {
                throw new SmartDataAccessorException("Could not get data from >" + webTarget.getUri() + "< retuned no >records<");
            }
            return records;
        } else {
            throw new SmartDataAccessorException("Could not access >" + webTarget.getUri() + "< returned status: " + response.getStatus());
        }
    }

    /**
     * Gets a list of available collections on the smartdata
     *
     * @param smartdataurl URL to the SmartData instance
     * @param storage Name of the storage to look at
     * @return List of collection names
     * @throws de.smartdata.lyser.data.SmartDataAccessorException
     */
    public List<String> fetchCollectons(String smartdataurl, String storage) throws SmartDataAccessorException {
        // Get information about file from SmartData
        WebTarget webTarget = WebTargetCreator.createWebTarget(
                smartdataurl + "/smartdata", "storage")
                .path("getCollections")
                .queryParam("name", storage);

        // Note request URI for documentation
        this.smartdataRequest = webTarget.getUri().toString();

        Response response = webTarget.request(MediaType.APPLICATION_JSON).get();
        String responseText = response.readEntity(String.class);
        if (Response.Status.OK.getStatusCode() == response.getStatus()) {
            JsonParser parser = Json.createParser(new StringReader(responseText));
            parser.next();
            JsonArray list = parser.getObject().getJsonArray("list");
            if (list == null) {
                throw new SmartDataAccessorException("Could not get storages from >" + webTarget.getUri() + "< retuned no >list<");
            }
            List<String> collections = new ArrayList<>();
            for (JsonValue curVal : list) {
                collections.add(curVal.asJsonObject().getString("name"));
            }
            return collections;
        } else {
            throw new SmartDataAccessorException("Could not access >" + webTarget.getUri() + "< returned status: " + responseText);
        }
    }

    /**
     * Get the size of the collection in byte
     *
     * @param smartdataurl SmartDatas URL
     * @param collection Collections name
     * @param storage Storages name
     * @return bytes of sotrage useage (estimated)
     * @throws SmartDataAccessorException
     */
    public long fetchSize(String smartdataurl, String collection, String storage) throws SmartDataAccessorException {
        // Local direct db access
        Connection con = this.getConnection();
        if (con != null) {
            try {
                // SQL-Abfrage mit einem Platzhalter für die Tabelle
                String sql = "SELECT pg_relation_size('\"" + storage + "\".\"" + collection + "\"')";
                long bytes;
                // Abfrage ausführen
                try (PreparedStatement preparedStatement = con.prepareStatement(sql); ResultSet resultSet = preparedStatement.executeQuery()) {
                    // Ergebnis verarbeiten
                    bytes = 0L;
                    if (resultSet.next()) {
                        bytes = resultSet.getLong(1);
                    }
                }
                return bytes;
            } catch (Exception ex) {
                throw new SmartDataAccessorException("Could not get data from >" + collection + "< an sql error occured: " + ex.getLocalizedMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new SmartDataAccessorException("Could not close db connection. Possible memory leak." + ex.getLocalizedMessage());
                }
            }
        }

        throw new SmartDataAccessorException("Could not get size for >" + smartdataurl + "<: Size fetching is currently not supported for databases accessable only over SmartData.");
    }

    public String getSmartdataRequest() {
        return this.smartdataRequest;
    }

    /**
     * Updated data to the database
     *
     * @param smartdataurl  URL of smartdata instance to use
     * @param collection    Collection to write in
     * @param storage       Storage to write in
     * @param dataSet       Dataset to use for update (only containing updateable attributes and id)
     * @throws SmartDataAccessorException
     */
    public void updateData(String smartdataurl, String collection, String storage, JsonObject dataSet) throws SmartDataAccessorException {
        de.ngi.logging.Logger.log();
        Connection con = this.getConnection();
        if (con != null) {
            StringBuilder updateQuery = new StringBuilder("UPDATE " + storage + "." + collection + " SET ");
            List<Object> values = new ArrayList<>();

            int index = 0;
            for (Map.Entry<String, JsonValue> curEntry : dataSet.entrySet()) {
                String column = curEntry.getKey();
                if (column.equals("id")) {
                    continue;
                }
                JsonValue value = curEntry.getValue();
                if (index > 0) {
                    updateQuery.append(", ");
                }
                updateQuery.append(column).append(" = ?");
                values.add(convertJsonValue(value)); // Umwandlung von JsonValue zu Java-Wert (z. B. String, Boolean, etc.)

                index++;
            }
            updateQuery.append(" WHERE id = ?");
            long setId = dataSet.getJsonNumber("id").longValue();

            String query = updateQuery.toString();

            try {
                PreparedStatement preparedStatement = con.prepareStatement(query);

                // Add data
                for (int i = 0; i < values.size(); i++) {
                    preparedStatement.setObject(i + 1, values.get(i));
                }
                // Set id
                preparedStatement.setObject(values.size() + 1, setId);

                int rowsInserted = preparedStatement.executeUpdate();
                if (rowsInserted > 0) {
                    de.ngi.logging.Logger.log("Succsessfull added dataset");
                } else {
                    de.ngi.logging.Logger.log("No new dataset created");
                }
            } catch (SQLException ex) {
                de.ngi.logging.Logger.log("Error saveing in database: " + ex.getLocalizedMessage());
                ex.printStackTrace();
                throw new SmartDataAccessorException("Could not create new dataset: " + ex.getLocalizedMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ex) {
                    throw new SmartDataAccessorException("Could not close db connection. Possible memory leak." + ex.getLocalizedMessage());
                }
            }
        } else {
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    private Object convertJsonValue(JsonValue jsonValue) {
        switch (jsonValue.getValueType()) {
            case STRING:
                String strValue = ((JsonString) jsonValue).getString();

                // Versuch, das Format ISO 8601 automatisch zu erkennen
                try {
                    Instant instant = Instant.parse(strValue); // z. B. "2025-07-04T07:45:00Z"
                    return Timestamp.from(instant); // korrekt für PostgreSQL
                } catch (DateTimeParseException e) {
                    // Kein gültiger Timestamp – bleibt String
                    return strValue;
                }
            case NUMBER:
                JsonNumber num = (JsonNumber) jsonValue;
                // Ganzzahl oder Fließkommazahl je nach Inhalt
                return num.isIntegral() ? num.longValue() : num.doubleValue();
            case TRUE:
                return true;
            case FALSE:
                return false;
            case NULL:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported JSON type: " + jsonValue.getValueType());
        }
    }
    /**
    * Gets all distinct days from the "ts" column in a collection
    *
    * @param smartdataurl SmartData's URL
    * @param collection Collection's name (table name)
    * @param storage Storage's name (schema name)
    * @return List of distinct dates as strings in YYYY-MM-DD format
    * @throws SmartDataAccessorException
    */
   public List<String> fetchDistinctDays(String smartdataurl, String collection, String storage) throws SmartDataAccessorException {
       List<String> distinctDays = new ArrayList<>();

       // Try local direct DB access first
       Connection con = this.getConnection();
       if (con != null) {
           try {
               // SQL query to get distinct dates from the ts column
               String sql = "SELECT DISTINCT CAST(\"ts\" AS DATE) AS day FROM \"" + storage + "\".\"" + collection + "\" ORDER BY day";

               try (PreparedStatement preparedStatement = con.prepareStatement(sql); ResultSet resultSet = preparedStatement.executeQuery()) {

                   while (resultSet.next()) {
                       Date date = resultSet.getDate("day");
                       if (date != null) {
                           distinctDays.add(date.toString()); // Returns in YYYY-MM-DD format
                       }
                   }

               }
               return distinctDays;
           } catch (SQLException ex) {
               throw new SmartDataAccessorException("Could not get distinct days from >" + collection
                   + "<: SQL error occurred: " + ex.getLocalizedMessage());
           } finally {
               try {
                   con.close();
               } catch (SQLException ex) {
                   throw new SmartDataAccessorException("Could not close DB connection. Possible memory leak: "
                       + ex.getLocalizedMessage());
               }
           }
       }

       // Use the fetchData method to get all ts values
       JsonArray datasets = this.fetchData(smartdataurl, collection, storage, "ts", null, null, null, null, null);
       // Extract distinct dates
       for (JsonValue value : datasets) {
           JsonObject obj = value.asJsonObject();
           if (obj.containsKey("ts")) {
               String tsValue = obj.getString("ts");
               try {
                   // Parse the timestamp and extract the date part
                   Instant instant = Instant.parse(tsValue);
                   // Locale datetime
                   String dateStr = instant.atZone(java.time.ZoneId.systemDefault())
                           .toLocalDate()
                           .toString();
                   // add if not already present
                   if (!distinctDays.contains(dateStr)) {
                       distinctDays.add(dateStr);
                   }
               } catch (DateTimeParseException e) {
                   // Skip if timestamp format is invalid

               }
           }
       }
       // Sort the dates
       distinctDays.sort(String::compareTo);
       return distinctDays;
   }
}
