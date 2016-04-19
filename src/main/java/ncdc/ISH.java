package ncdc;

import java.io.Serializable;

/**
 * @see https://data.noaa.gov/dataset/integrated-surface-global-hourly-data
 * @see http://www1.ncdc.noaa.gov/pub/data/ish/ish-format-document.pdf
 * @see ftp://ftp.ncdc.noaa.gov/pub/data/noaa/ish-abbreviated.txt
 * @see ftp://ftp.ncdc.noaa.gov/pub/data/noaa/ishJava.java
 * 
 * @author Ali Shakiba
 */
public class ISH {

    public static ISHBasic parseBasic(String line, ISHBasic ish) {
        return ish.read(line);
    }

    public static ISHBasic parseBasic(String line) {
        return parseBasic(line, new ISHBasic());
    }

    public static ISHFull parseFull(String line, ISHFull ish) {
        return ish.read(line);
    }

    public static ISHFull parseFull(String line) {
        return parseFull(line, new ISHFull());
    }

    // Used for empty, invalid or missing values
    private static final String nil = "";

    /**
     * Includes only Control and Mandatory data sections to enhance reading
     * performance and memory usage.
     * 
     * @see ISHFull
     */
    public static class ISHBasic implements Serializable {

        // Control Data Section
        public String usaf;
        public String wban;
        public String year;
        public String month;
        public String day;
        public String hour;
        public String minute;
        public String latitude;
        public String longitude;
        public String elevation;

        // Mandatory Data Section
        public String windDirection;
        public String windSpeed;
        public String cloudCeiling;
        public String visibility;
        public String temp;
        public String dewp;
        /** Sea Level Pressure */
        public String pressure;

        private ISHBasic read(String line) {

            // Control Data Section

            this.usaf = line.substring(4, 10);

            this.wban = line.substring(10, 15);
            if (this.wban.equals("99999")) {
                this.wban = nil;
            }

            this.year = line.substring(15, 19);
            this.month = line.substring(19, 21);
            this.day = line.substring(21, 23);
            this.hour = line.substring(23, 25);
            this.minute = line.substring(25, 27);

            this.latitude = line.substring(28, 34);
            this.longitude = line.substring(34, 41);
            this.elevation = line.substring(46, 51);

            // Mandatory Data Section

            this.windDirection = line.substring(60, 63);
            if (this.windDirection.equals("999")) {
                this.windDirection = nil;
            }

            this.windSpeed = line.substring(65, 69);
            if (this.windSpeed.equals("9999")) {
                this.windSpeed = nil;
            }

            this.cloudCeiling = line.substring(70, 75);
            if (this.cloudCeiling.equals("99999")) {
                this.cloudCeiling = nil;
            }

            this.visibility = line.substring(78, 84);
            if (this.visibility.equals("999999")) {
                this.visibility = nil;
            }

            this.temp = line.substring(87, 92);
            if (this.temp.equals("9999")) {
                this.temp = nil;
            }

            this.dewp = line.substring(93, 98);
            if (this.dewp.equals("9999")) {
                this.dewp = nil;
            }

            this.pressure = line.substring(99, 104);
            if (this.pressure.equals("99999")) {
                this.pressure = nil;
            }

            return this;
        }

        private static final long serialVersionUID = -1235118486740642778L;
    }

    /**
     * @see ISHBasic
     */
    public static class ISHFull extends ISHBasic implements Serializable {

        // OC1
        public String windGust;

        // AJ1
        public String snowDepth;

        // AY1
        /** Past Weather */
        public String PW;

        // MW1-4
        /** Present Manual Weather */
        public String MW1, MW2, MW3, MW4;

        // AW1-4
        /** Present Automated Weather */
        public String AW1, AW2, AW3, AW4;

        // GF1
        public String skyCoverage;
        public String lowCloud;
        public String midCloud;
        public String highCloud;

        // MA1
        /** Station Pressure and Altitude */
        public String stAlt, stPrs;

        // KA1-2
        public String maxTemp, minTemp;

        // AA1-4
        public String precip01h;
        public String precip01ht;
        public String precip06h;
        public String precip06ht;
        public String precip24h;
        public String precip24ht;
        public String precipxh;
        public String precipxht;

        private ISHFull read(String line) {
            super.read(line);

            // start of REM section
            int iREM = line.indexOf("REM");
            if (iREM == -1) {
                iREM = 9999;
            }

            this.PW = findStr(line, 0, iREM, "AY1", 3, 4, nil);

            this.MW1 = findStr(line, 0, iREM, "MW1", 3, 5, nil);
            this.MW2 = findStr(line, 0, iREM, "MW2", 3, 5, nil);
            this.MW3 = findStr(line, 0, iREM, "MW3", 3, 5, nil);
            this.MW4 = findStr(line, 0, iREM, "MW4", 3, 5, nil);

            this.AW1 = findStr(line, 0, iREM, "AW1", 3, 5, nil);
            this.AW2 = findStr(line, 0, iREM, "AW2", 3, 5, nil);
            this.AW3 = findStr(line, 0, iREM, "AW3", 3, 5, nil);
            this.AW4 = findStr(line, 0, iREM, "AW4", 3, 5, nil);

            this.windGust = findStr(line, 0, iREM, "OC1", 3, 7, nil);
            if (this.windGust.equals("9999")) {
                this.windGust = nil;
            }

            this.snowDepth = findStr(line, 0, iREM, "AJ1", 3, 7, nil);
            if (this.snowDepth.equals("9999")) {
                this.snowDepth = nil;
            }

            this.skyCoverage = this.lowCloud = this.midCloud = this.highCloud = nil;
            int iGF1 = indexOf(line, 0, iREM, "GF1");
            if (iGF1 >= 0) {
                this.skyCoverage = line.substring(iGF1 + 3, iGF1 + 5);
                if (this.skyCoverage.equals("99")) {
                    this.skyCoverage = nil;
                }

                this.lowCloud = line.substring(iGF1 + 11, iGF1 + 13);
                if (this.lowCloud.equals("99")) {
                    this.lowCloud = nil;
                } else {
                    this.lowCloud = this.lowCloud.substring(1, 2);
                }

                this.midCloud = line.substring(iGF1 + 20, iGF1 + 22);
                if (this.midCloud.equals("99")) {
                    this.midCloud = nil;
                } else {
                    this.midCloud = this.midCloud.substring(1, 2);
                }

                this.highCloud = line.substring(iGF1 + 23, iGF1 + 25);
                if (this.highCloud.equals("99")) {
                    this.highCloud = nil;
                } else {
                    this.highCloud = this.highCloud.substring(1, 2);
                }
            }

            this.stAlt = this.stPrs = nil;
            int iMA1 = indexOf(line, 0, iREM, "MA1");
            if (iMA1 >= 0) {
                this.stAlt = line.substring(iMA1 + 3, iMA1 + 8);
                if (this.stAlt.equals("99999")) {
                    this.stAlt = nil;
                }
                this.stPrs = line.substring(iMA1 + 9, iMA1 + 14);
                if (this.stPrs.equals("99999")) {
                    this.stPrs = nil;
                }
            }

            this.maxTemp = this.minTemp = nil;
            int iKA1 = indexOf(line, 0, iREM, "KA1");
            if (iKA1 >= 0) {
                String temp = line.substring(iKA1 + 7, iKA1 + 12);
                if (temp.equals("+9999")) {
                    temp = nil;
                } else {
                    String code = line.substring(iKA1 + 6, iKA1 + 7);
                    this.setTemp(temp, code);
                }
            }
            int iKA2 = indexOf(line, 0, iREM, "KA2");
            if (iKA2 >= 0) {
                String temp = line.substring(iKA2 + 7, iKA2 + 12);
                if (temp.equals("+9999")) {
                    temp = nil;
                } else {
                    String code = line.substring(iKA2 + 6, iKA2 + 7);
                    this.setTemp(temp, code);
                }
            }

            this.precip01h = this.precip06h = this.precip24h = this.precipxh = nil;
            this.precip01ht = this.precip06ht = this.precip24ht = this.precipxht = nil;
            int iAA1 = indexOf(line, 0, iREM, "AA1");
            if (iAA1 >= 0) {
                String precip = line.substring(iAA1 + 5, iAA1 + 9);
                if (precip.equals("9999")) {
                    precip = nil;
                } else {
                    String hours = line.substring(iAA1 + 3, iAA1 + 5);
                    String trace = line.substring(iAA1 + 9, iAA1 + 10);
                    this.setPrecip(precip, hours, trace);
                }
            }
            int iAA2 = indexOf(line, 0, iREM, "AA2");
            if (iAA2 >= 0) {
                String precip = line.substring(iAA2 + 5, iAA2 + 9);
                if (precip.equals("9999")) {
                    precip = nil;
                } else {
                    String hours = line.substring(iAA2 + 3, iAA2 + 5);
                    String trace = line.substring(iAA2 + 9, iAA2 + 10);
                    this.setPrecip(precip, hours, trace);
                }
            }
            int iAA3 = indexOf(line, 0, iREM, "AA3");
            if (iAA3 >= 0) {
                String precip = line.substring(iAA3 + 5, iAA3 + 9);
                if (precip.equals("9999")) {
                    precip = nil;
                } else {
                    String hours = line.substring(iAA3 + 3, iAA3 + 5);
                    String trace = line.substring(iAA3 + 9, iAA3 + 10);
                    this.setPrecip(precip, hours, trace);
                }
            }
            int iAA4 = indexOf(line, 0, iREM, "AA4");
            if (iAA4 >= 0) {
                String precip = line.substring(iAA4 + 5, iAA4 + 9);
                if (precip.equals("9999")) {
                    precip = nil;
                } else {
                    String hours = line.substring(iAA4 + 3, iAA4 + 5);
                    String trace = line.substring(iAA4 + 9, iAA4 + 10);
                    this.setPrecip(precip, hours, trace);
                }
            }

            return this;
        }

        private void setTemp(String temp, String code) {
            if (code.equals("N")) {
                minTemp = temp;
            } else if (code.equals("M")) {
                maxTemp = temp;
            }
        }

        private void setPrecip(String precip, String hours, String trace) {
            // precip = String.format("%5s", precip);
            // trace = trace.equals("2") ? "T" : nil;
            switch (hours) {
            case "01":
                precip01h = precip;
                precip01ht = trace;
                break;
            case "06":
                precip06h = precip;
                precip06ht = trace;
                break;
            case "24":
                precip24h = precip;
                precip24ht = trace;
                break;
            default:
                precipxh = precip;
                precipxht = trace;
                break;
            }
        }

        private static final long serialVersionUID = -2723382536800365942L;
    }

    private static int indexOf(String line, int from, int to, String tag) {
        int i = line.indexOf(tag, from);
        return i < to ? i : -1;
    }

    private static String findStr(String line, int searchFrom, int searchTo,
            String tag, int from, int to, String fallback) {
        int i = indexOf(line, searchFrom, searchTo, tag);
        if (i >= 0) {
            return line.substring(i + from, i + to);
        }
        return fallback;
    }

}
