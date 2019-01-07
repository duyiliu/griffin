package com.saicmotor.datagovern.profile.utils

import com.saicmotor.datagovern.profile.global.CustomDate
import org.apache.commons.lang3.time.DateUtils
import java.math.BigDecimal
import java.text.DateFormat
import java.text.DecimalFormat
import java.text.NumberFormat
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Date
import java.util
import java.util.regex.Pattern


/**
  * @author chimney
  */
object DataFormatUtils {
  private val NUMERIC_FORMATS_REGEX = "^(-?\\d+)(\\.\\d+)?$"
  private val DATATIME_FORMATS_REGX = "^(?:19|20)[0-9][0-9]-(?:(?:0[1-9])|(?:1[0-2]))-(?:(?:[0-2][1-9])|(?:[1-3][0-1]))( (?:(?:[0-2][0-3])|(?:[0-1][0-9]))(:[0-5][0-9](:[0-5][0-9])?)?)?$"

  def offer(s: String): Any = {
    if (Pattern.matches(NUMERIC_FORMATS_REGEX, s)) return new BigDecimal(s)
    if (Pattern.matches(DATATIME_FORMATS_REGX, s)) {
      val customDate = new  CustomDate()
      customDate.setValue(s)
      return customDate
    }
    s
  }
}
