package com.alibaba.datax.plugin.reader.mysqlreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;

public class MysqlReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.MySql;

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            Integer userConfigedFetchSize = this.originalConfig.getInt(Constant.FETCH_SIZE);
            if (userConfigedFetchSize != null) {
                LOG.warn("对 mysqlreader 不需要配置 fetchSize, mysqlreader 将会忽略这项配置. 如果您不想再看到此警告,请去除fetchSize 配置.");
            }

            this.originalConfig.set(Constant.FETCH_SIZE, Integer.MIN_VALUE);

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
            System.out.println(111);
        }

        @Override
        public void preCheck(){
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig,DATABASE_TYPE);

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }

    }

    private static String getGeometryFromInputStream(InputStream inputStream) throws Exception {

        if (inputStream != null) {
            // 把二进制流转成字节数组
            //convert the stream to a byte[] array
            //so it can be passed to the WKBReader
            byte[] buffer = new byte[inputStream.available()];

            int bytesRead = 0;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            // 得到字节数组
            byte[] geometryAsBytes = baos.toByteArray();
            // 字节数组小于5，说明geometry有问题
            if (geometryAsBytes.length < 5) {
                throw new Exception("Invalid geometry inputStream - less than five bytes");
            }
            String s = DatatypeConverter.printHexBinary(geometryAsBytes);

            return s;
        }

        return "";
    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;
        private   Logger logger = LoggerFactory.getLogger(Task.class);
        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(DATABASE_TYPE,super.getTaskGroupId(), super.getTaskId()){

                @Override
                protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding, TaskPluginCollector taskPluginCollector) {

                    Record record = recordSender.createRecord();
                    try {
                        for (int i = 1; i <= columnNumber; i++) {

                            String cTypeName =  metaData.getColumnTypeName(i);

                            if("GEOMETRY".equalsIgnoreCase(cTypeName)){
                                InputStream inputStream = rs.getBinaryStream(i);
                                // 转换为geometry对象
                                String geometryFromInputStream = getGeometryFromInputStream(inputStream);
                                record.addColumn(new StringColumn(geometryFromInputStream));
                                continue;
                            }

                            switch (metaData.getColumnType(i)) {

                                case Types.CHAR:
                                case Types.NCHAR:
                                case Types.VARCHAR:
                                case Types.LONGVARCHAR:
                                case Types.NVARCHAR:
                                case Types.LONGNVARCHAR:
                                    String rawData;
                                    if(StringUtils.isBlank(mandatoryEncoding)){
                                        rawData = rs.getString(i);
                                    }else{
                                        rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY :
                                                rs.getBytes(i)), mandatoryEncoding);
                                    }
                                    record.addColumn(new StringColumn(rawData));
                                    break;

                                case Types.CLOB:
                                case Types.NCLOB:
                                    record.addColumn(new StringColumn(rs.getString(i)));
                                    break;

                                case Types.SMALLINT:
                                case Types.TINYINT:
                                case Types.INTEGER:
                                case Types.BIGINT:
                                    record.addColumn(new LongColumn(rs.getString(i)));
                                    break;

                                case Types.NUMERIC:
                                case Types.DECIMAL:
                                    record.addColumn(new DoubleColumn(rs.getString(i)));
                                    break;

                                case Types.FLOAT:
                                case Types.REAL:
                                case Types.DOUBLE:
                                    record.addColumn(new DoubleColumn(rs.getString(i)));
                                    break;

                                case Types.TIME:
                                    record.addColumn(new DateColumn(rs.getTime(i)));
                                    break;

                                // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                                case Types.DATE:
                                    if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                                        record.addColumn(new LongColumn(rs.getInt(i)));
                                    } else {
                                        record.addColumn(new DateColumn(rs.getDate(i)));
                                    }
                                    break;

                                case Types.TIMESTAMP:
                                    record.addColumn(new DateColumn(rs.getTimestamp(i)));
                                    break;

                                case Types.BINARY:
                                case Types.VARBINARY:
                                case Types.BLOB:
                                case Types.LONGVARBINARY:
                                    record.addColumn(new BytesColumn(rs.getBytes(i)));
                                    break;

                                // warn: bit(1) -> Types.BIT 可使用BoolColumn
                                // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                                case Types.BOOLEAN:
                                case Types.BIT:
                                    record.addColumn(new BoolColumn(rs.getBoolean(i)));
                                    break;

                                case Types.NULL:
                                    String stringData = null;
                                    if(rs.getObject(i) != null) {
                                        stringData = rs.getObject(i).toString();
                                    }
                                    record.addColumn(new StringColumn(stringData));
                                    break;

                                default:
                                    throw DataXException
                                            .asDataXException(
                                                    DBUtilErrorCode.UNSUPPORTED_TYPE,
                                                    String.format(
                                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                            metaData.getColumnName(i),
                                                            metaData.getColumnType(i),
                                                            metaData.getColumnClassName(i)));
                            }
                        }
                    } catch (Exception e) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("read data " + record.toString()
                                    + " occur exception:", e);
                        }
                        //TODO 这里识别为脏数据靠谱吗？
                        taskPluginCollector.collectDirtyRecord(record, e);
                        if (e instanceof DataXException) {
                            throw (DataXException) e;
                        }
                    }
                    return record;
                }
            };
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);

        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(Constant.FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }

}
