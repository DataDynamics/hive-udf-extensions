package io.datadynamics.hive.udf.string;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;

@Description(name = "substr_euckr",
        value = "_FUNC_(str, trim, pos[, len]) - returns the substring of str that"
                + " starts at pos and is of length len",
        extended = "pos is a 1-based index. If pos<0 the starting position is"
                + " determined by counting backwards from the end of str.\n"
                + "Example:\n "
                + "  > SELECT _FUNC_('Facebook', true, 5) FROM src LIMIT 1;\n"
                + "  'book'\n"
                + "  > SELECT _FUNC_('Facebook', false, -5) FROM src LIMIT 1;\n"
                + "  'ebook'\n"
                + "  > SELECT _FUNC_('Facebook', false, 5, 1) FROM src LIMIT 1;\n"
                + "  'b'")
public class UDFSubStringEucKR extends UDF {

    public Text evaluate(Text text, IntWritable pos, IntWritable len, BooleanWritable isTrim) {
        if ((text == null) || (pos == null) || (len == null)) {
            return null;
        }

        try {
            byte[] value = text.toString().getBytes("EUC-KR");
            int longPos = pos.get();
            int longLen = len.get();
            boolean trim = isTrim != null && isTrim.get();
            String substring = substring(new String(value, "EUC-KR"), longPos + 1, longLen);

            if (trim) {
                return new Text(substring.trim());
            } else {
                return new Text(substring);
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static String substring(String text, int beginBytes, int endBytes) throws UnsupportedEncodingException {
        // 1. 입력값 유효성 검사
        if (text == null || text.isEmpty()) {
            return "";
        }
        if (beginBytes < 0) {
            beginBytes = 0;
        }
        if (endBytes < 0 || beginBytes >= endBytes) {
            return "";
        }

        // 2. 로직 구현
        StringBuilder result = new StringBuilder();
        int currentBytes = 0; // 현재까지 누적된 바이트

        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);

            // 현재 문자의 바이트 크기 계산 (EUC-KR 기준)
            int charBytes = (ch > 127) ? 2 : 1;

            // --- 핵심 로직 ---
            // 현재 문자가 지정된 범위 [beginBytes, endBytes) 안에 "온전히" 들어가는지 확인합니다.
            // 조건 1: 현재 문자의 시작 위치(currentBytes)가 beginBytes보다 크거나 같아야 합니다.
            // 조건 2: 현재 문자의 끝 위치(currentBytes + charBytes)가 endBytes보다 작거나 같아야 합니다.
            if (currentBytes >= beginBytes && (currentBytes + charBytes) <= endBytes) {
                result.append(ch);
            }

            // 누적 바이트를 업데이트합니다.
            currentBytes += charBytes;

            // 최적화: 누적 바이트가 이미 endBytes를 넘어섰다면 더 이상 순회할 필요가 없습니다.
            if (currentBytes >= endBytes) {
                break;
            }
        }

        return new String(result.toString().getBytes("EUC-KR"));
    }

}