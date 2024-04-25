/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.util;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.text.StringSearch;
import com.ibm.icu.util.ULocale;
import com.ibm.icu.text.Collator;

import org.apache.spark.SparkException;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Static entry point for collation aware string functions.
 * Provides functionality to the UTF8String object which respects defined collation settings.
 */
public final class CollationFactory {
  /**
   * Entry encapsulating all information about a collation.
   */
  public static class Collation {
    public final String collationName;
    public final Collator collator;
    public final Comparator<UTF8String> comparator;

    /**
     * Version of the collation. This is the version of the ICU library Collator.
     * For non-ICU collations (e.g. UTF8 Binary) the version is set to "1.0".
     * When using ICU Collator this version is exposed through collator.getVersion().
     * Whenever the collation is updated, the version should be updated as well or kept
     * for backwards compatibility.
     */
    public final String version;

    /**
     * Collation sensitive hash function. Output for two UTF8Strings will be the same if they are
     * equal according to the collation.
     */
    public final ToLongFunction<UTF8String> hashFunction;

    /**
     * Potentially faster way than using comparator to compare two UTF8Strings for equality.
     * Falls back to binary comparison if the collation is binary.
     */
    public final BiFunction<UTF8String, UTF8String, Boolean> equalsFunction;

    /**
     * Support for Binary Equality implies that it is possible to check equality on
     * byte by byte level. This allows for the usage of binaryEquals call on UTF8Strings
     * which is more performant than calls to external ICU library.
     */
    public final boolean supportsBinaryEquality;
    /**
     * Support for Binary Ordering implies that it is possible to check equality and ordering on
     * byte by byte level. This allows for the usage of binaryEquals and binaryCompare calls on
     * UTF8Strings which is more performant than calls to external ICU library. Support for
     * Binary Ordering implies support for Binary Equality.
     */
    public final boolean supportsBinaryOrdering;

    /**
     * Support for Lowercase Equality implies that it is possible to check equality on
     * byte by byte level, but only after calling "UTF8String.toLowerCase" on both arguments.
     * This allows custom collation support for UTF8_BINARY_LCASE collation in various Spark
     * expressions, as this particular collation is not supported by the external ICU library.
     */
    public final boolean supportsLowercaseEquality;

    public Collation(
        String collationName,
        Collator collator,
        Comparator<UTF8String> comparator,
        String version,
        ToLongFunction<UTF8String> hashFunction,
        boolean supportsBinaryEquality,
        boolean supportsBinaryOrdering,
        boolean supportsLowercaseEquality) {
      this.collationName = collationName;
      this.collator = collator;
      this.comparator = comparator;
      this.version = version;
      this.hashFunction = hashFunction;
      this.supportsBinaryEquality = supportsBinaryEquality;
      this.supportsBinaryOrdering = supportsBinaryOrdering;
      this.supportsLowercaseEquality = supportsLowercaseEquality;

      // De Morgan's Law to check supportsBinaryOrdering => supportsBinaryEquality
      assert(!supportsBinaryOrdering || supportsBinaryEquality);
      // No Collation can simultaneously support binary equality and lowercase equality
      assert(!supportsBinaryEquality || !supportsLowercaseEquality);

      if (supportsBinaryEquality) {
        this.equalsFunction = UTF8String::equals;
      } else {
        this.equalsFunction = (s1, s2) -> this.comparator.compare(s1, s2) == 0;
      }
    }

    /**
     * collation id (32-bit integer) layout:
     * bit 31:    0 = predefined collation, 1 = user-defined collation
     * bit 30-29: 00 = utf8-binary, 01 = ICU, 10 = indeterminate (without spec implementation)
     * bit 28:    0 for utf8-binary / 0 = case-sensitive, 1 = case-insensitive for ICU
     * bit 27:    0 for utf8-binary / 0 = accent-sensitive, 1 = accent-insensitive for ICU
     * bit 26-25: zeroes, reserved for punctuation sensitivity
     * bit 24-23: zeroes, reserved for first letter preference
     * bit 22-21: 00 = unspecified, 01 = to-lower, 10 = to-upper
     * bit 20-19: zeroes, reserved for space trimming
     * bit 18-17: zeroes, reserved for version
     * bit 16-12: zeroes
     * bit 11-0:  zeroes for utf8-binary / locale id for ICU
     */
    private abstract static class CollationSpec {
      protected enum ImplementationProvider {
        UTF8_BINARY, ICU, INDETERMINATE
      }

      protected enum CaseSensitivity {
        CS, CI
      }

      protected enum AccentSensitivity {
        AS, AI
      }

      protected enum CaseConversion {
        UNSPECIFIED, LCASE, UCASE
      }

      protected static final int IMPLEMENTATION_PROVIDER_OFFSET = 29;
      protected static final int IMPLEMENTATION_PROVIDER_MASK = 0b11;
      protected static final int CASE_SENSITIVITY_OFFSET = 28;
      protected static final int CASE_SENSITIVITY_MASK = 0b1;
      protected static final int ACCENT_SENSITIVITY_OFFSET = 27;
      protected static final int ACCENT_SENSITIVITY_MASK = 0b1;
      protected static final int CASE_CONVERSION_OFFSET = 21;
      protected static final int CASE_CONVERSION_MASK = 0b11;
      protected static final int LOCALE_OFFSET = 0;
      protected static final int LOCALE_MASK = 0x0FFF;

      protected static final int INDETERMINATE_COLLATION_ID =
        ImplementationProvider.INDETERMINATE.ordinal() << IMPLEMENTATION_PROVIDER_OFFSET;

      protected final CaseSensitivity caseSensitivity;
      protected final AccentSensitivity accentSensitivity;
      protected final CaseConversion caseConversion;
      protected final String locale;
      protected final int collationId;

      protected CollationSpec(
          String locale,
          CaseSensitivity caseSensitivity,
          AccentSensitivity accentSensitivity,
          CaseConversion caseConversion) {
        this.locale = locale;
        this.caseSensitivity = caseSensitivity;
        this.accentSensitivity = accentSensitivity;
        this.caseConversion = caseConversion;
        this.collationId = getCollationId();
      }

      private static final Map<Integer, Collation> collationMap = new ConcurrentHashMap<>();

      public static Collation fetchCollation(int collationId) throws SparkException {
        if (collationMap.containsKey(collationId)) {
          return collationMap.get(collationId);
        } else {
          CollationSpec spec;
          int implementationProviderOrdinal =
            (collationId >> IMPLEMENTATION_PROVIDER_OFFSET) & IMPLEMENTATION_PROVIDER_MASK;
          if (implementationProviderOrdinal >= ImplementationProvider.values().length) {
            throw SparkException.internalError("Invalid implementation provider");
          } else {
            ImplementationProvider implementationProvider = ImplementationProvider.values()[
              implementationProviderOrdinal];
            if (implementationProvider == ImplementationProvider.UTF8_BINARY) {
              spec = CollationSpecUTF8Binary.fromCollationId(collationId);
            } else if (implementationProvider == ImplementationProvider.ICU) {
              spec = CollationSpecICU.fromCollationId(collationId);
            } else {
              throw SparkException.internalError("Cannot instantiate indeterminate collation");
            }
            Collation collation = spec.buildCollation();
            collationMap.put(collationId, collation);
            return collation;
          }
        }
      }

      public static int collationNameToId(String originalCollationName) throws SparkException {
        String collationName = originalCollationName.toUpperCase();
        try {
          if (collationName.startsWith("UTF8_BINARY")) {
            return CollationSpecUTF8Binary.collationNameToId(collationName);
          } else {
            return CollationSpecICU.collationNameToId(collationName);
          }
        } catch (SparkException e) {
          throw new SparkException(
            "COLLATION_INVALID_NAME",
            SparkException.constructMessageParams(Map.of("collationName", originalCollationName)),
            e);
        }
      }

      protected static int parseSpecifiers(String specString) throws SparkException {
        int specifiers = 0;
        String[] parts = specString.split("_");
        for (String part : parts) {
          if (!part.isEmpty()) {
            if (part.equals("UNSPECIFIED") || part.equals("INDETERMINATE")) {
              throw new SparkException(part + " collation specifier reserved for internal use");
            } else if (Arrays.stream(CaseSensitivity.values()).anyMatch(
                (s) -> s.toString().equals(part))) {
              specifiers |=
                CaseSensitivity.valueOf(part).ordinal() << CASE_SENSITIVITY_OFFSET;
            } else if (Arrays.stream(AccentSensitivity.values()).anyMatch(
                (s) -> part.equals(s.toString()))) {
              specifiers |=
                AccentSensitivity.valueOf(part).ordinal() << ACCENT_SENSITIVITY_OFFSET;
            } else if (Arrays.stream(CaseConversion.values()).anyMatch(
                (s) -> part.equals(s.toString()))) {
              specifiers |=
                CaseConversion.valueOf(part).ordinal() << CASE_CONVERSION_OFFSET;
            } else {
              throw new SparkException("Invalid collation specifier value " + part);
            }
          }
        }
        return specifiers;
      }

      protected abstract int getCollationId();
      protected abstract Collation buildCollation();
    }

    private static class CollationSpecUTF8Binary extends CollationSpec {

      public static final int UTF8_BINARY_COLLATION_ID =
        new CollationSpecUTF8Binary(CaseConversion.UNSPECIFIED).getCollationId();
      public static final int UTF8_BINARY_LCASE_COLLATION_ID =
        new CollationSpecUTF8Binary(CaseConversion.LCASE).getCollationId();
      public static Collation UTF8_BINARY_COLLATION =
        new CollationSpecUTF8Binary(CaseConversion.UNSPECIFIED).buildCollation();

      private CollationSpecUTF8Binary(CaseConversion caseConversion) {
        super(null, CaseSensitivity.CS, AccentSensitivity.AS, caseConversion);
      }

      public static int collationNameToId(String collationName) throws SparkException {
        int collationId = 0;
        int specifiers = CollationSpec.parseSpecifiers(
          collationName.substring("UTF8_BINARY".length()));
        collationId |= specifiers & (CASE_CONVERSION_MASK << CASE_CONVERSION_OFFSET);
        return collationId;
      }

      @Override
      protected int getCollationId() {
        int collationId = 0;
        collationId |= caseConversion.ordinal() << CASE_CONVERSION_OFFSET;
        return collationId;
      }

      public static CollationSpecUTF8Binary fromCollationId(int collationId)
          throws SparkException {
        int originalCollationId = collationId;
        int caseConversionOrdinal =
          (collationId >> CASE_CONVERSION_OFFSET) & CASE_CONVERSION_MASK;
        collationId ^= caseConversionOrdinal << CASE_CONVERSION_OFFSET;
        if (collationId != 0 || caseConversionOrdinal >= CaseConversion.values().length) {
          throw SparkException.internalError("Invalid UTF8_BINARY collation id " +
            originalCollationId);
        } else {
          CaseConversion caseConversion = CaseConversion.values()[caseConversionOrdinal];
          return new CollationSpecUTF8Binary(caseConversion);
        }
      }

      @Override
      protected Collation buildCollation() {
        Comparator<UTF8String> comparator;
        if (collationId == UTF8_BINARY_COLLATION_ID) {
          comparator = UTF8String::binaryCompare;
        } else if (collationId == UTF8_BINARY_LCASE_COLLATION_ID) {
          comparator = UTF8String::compareLowerCase;
        } else {
          comparator = (s1, s2) -> {
            UTF8String convertedS1 = caseConversion(s1);
            UTF8String convertedS2 = caseConversion(s2);
            return convertedS1.binaryCompare(convertedS2);
          };
        }
        return new Collation(
          collationName(),
          null,
          comparator,
          "1.0",
          s -> (long) caseConversion(s).hashCode(),
          collationId == UTF8_BINARY_COLLATION_ID,
          collationId == UTF8_BINARY_COLLATION_ID,
          collationId == UTF8_BINARY_LCASE_COLLATION_ID
        );
      }

      private UTF8String caseConversion(UTF8String s) {
        if (caseConversion == CaseConversion.LCASE) {
          return s.toLowerCase();
        } else if (caseConversion == CaseConversion.UCASE) {
          return s.toUpperCase();
        } else {
          return s;
        }
      }

      private String collationName() {
        StringBuilder builder = new StringBuilder();
        builder.append("UTF8_BINARY");
        if (caseConversion != CaseConversion.UNSPECIFIED) {
          builder.append('_');
          builder.append(caseConversion.toString());
        }
        return builder.toString();
      }
    }

    private static class CollationSpecICU extends CollationSpec {

      private static final String[] ICULocaleNames;
      private static final Map<String, ULocale> ICULocaleMap = new HashMap<>();
      private static final Map<String, String> ICULocaleMapUppercase = new HashMap<>();
      private static final Map<String, Integer> ICULocaleToId = new HashMap<>();

      static {
        ICULocaleMap.put("UNICODE", ULocale.ROOT);
        ULocale[] locales = Collator.getAvailableULocales();
        for (ULocale locale : locales) {
          if (locale.getVariant().isEmpty()) {
            String language = locale.getLanguage();
            assert (!language.isEmpty());
            StringBuilder builder = new StringBuilder(language);
            String script = locale.getScript();
            if (!script.isEmpty()) {
              builder.append('_');
              builder.append(script);
            }
            String country = locale.getISO3Country();
            if (!country.isEmpty()) {
              builder.append('_');
              builder.append(country);
            }
            String localeName = builder.toString();
            assert (!ICULocaleMap.containsKey(localeName));
            ICULocaleMap.put(localeName, locale);
          }
        }
        for (String localeName : ICULocaleMap.keySet()) {
          String localeUppercase = localeName.toUpperCase();
          assert (!ICULocaleMapUppercase.containsKey(localeUppercase));
          ICULocaleMapUppercase.put(localeUppercase, localeName);
        }
        ICULocaleNames = ICULocaleMap.keySet().toArray(new String[0]);
        Arrays.sort(ICULocaleNames);
        assert (ICULocaleNames.length <= (1 << 16));
        for (int i = 0; i < ICULocaleNames.length; i++) {
          ICULocaleToId.put(ICULocaleNames[i], i);
        }
      }

      public static final int UNICODE_COLLATION_ID =
        new CollationSpecICU("UNICODE", CaseSensitivity.CS, AccentSensitivity.AS,
          CaseConversion.UNSPECIFIED).getCollationId();
      public static final int UNICODE_CI_COLLATION_ID =
        new CollationSpecICU("UNICODE", CaseSensitivity.CI, AccentSensitivity.AS,
          CaseConversion.UNSPECIFIED).getCollationId();

      private CollationSpecICU(String locale, CaseSensitivity caseSensitivity,
          AccentSensitivity accentSensitivity, CaseConversion caseConversion) {
        super(locale, caseSensitivity, accentSensitivity, caseConversion);
      }

      public static int collationNameToId(String collationName) throws SparkException {
        int lastPos = -1;
        for (int i = 1; i <= collationName.length(); i++) {
          String localeName = collationName.substring(0, i);
          if (ICULocaleMapUppercase.containsKey(localeName)) {
            lastPos = i;
          }
        }
        if (lastPos == -1) {
          throw new SparkException("Invalid locale in collation name value " + collationName);
        } else {
          int collationId = 0;
          collationId |= ImplementationProvider.ICU.ordinal() << IMPLEMENTATION_PROVIDER_OFFSET;
          collationId |= CollationSpec.parseSpecifiers(collationName.substring(lastPos));
          String normalizedLocaleName = ICULocaleMapUppercase.get(
            collationName.substring(0, lastPos));
          collationId |= ICULocaleToId.get(normalizedLocaleName) << LOCALE_OFFSET;
          return collationId;
        }
      }

      public static CollationSpecICU fromCollationId(int collationId) throws SparkException {
        int originalCollationId = collationId;
        int caseSensitivityOrdinal =
          (collationId >> CASE_SENSITIVITY_OFFSET) & CASE_SENSITIVITY_MASK;
        int accentSensitivityOrdinal =
          (collationId >> ACCENT_SENSITIVITY_OFFSET) & ACCENT_SENSITIVITY_MASK;
        int caseConversionOrdinal =
          (collationId >> CASE_CONVERSION_OFFSET) & CASE_CONVERSION_MASK;
        int localeOrdinal = (collationId >> LOCALE_OFFSET) & LOCALE_MASK;
        collationId ^= ImplementationProvider.ICU.ordinal() << IMPLEMENTATION_PROVIDER_OFFSET;
        collationId ^= caseSensitivityOrdinal << CASE_SENSITIVITY_OFFSET;
        collationId ^= accentSensitivityOrdinal << ACCENT_SENSITIVITY_OFFSET;
        collationId ^= caseConversionOrdinal << CASE_CONVERSION_OFFSET;
        collationId ^= localeOrdinal << LOCALE_OFFSET;
        if (collationId != 0 || caseConversionOrdinal >= CaseConversion.values().length ||
            localeOrdinal >= ICULocaleNames.length) {
          throw SparkException.internalError("Invalid ICU collation id " + originalCollationId);
        } else {
          CaseSensitivity caseSensitivity = CaseSensitivity.values()[caseSensitivityOrdinal];
          AccentSensitivity accentSensitivity =
            AccentSensitivity.values()[accentSensitivityOrdinal];
          CaseConversion caseConversion = CaseConversion.values()[caseConversionOrdinal];
          String locale = ICULocaleNames[localeOrdinal];
          return new CollationSpecICU(locale, caseSensitivity, accentSensitivity, caseConversion);
        }
      }

      @Override
      protected int getCollationId() {
        int collationId = 0;
        collationId |= ImplementationProvider.ICU.ordinal() << IMPLEMENTATION_PROVIDER_OFFSET;
        collationId |= caseSensitivity.ordinal() << CASE_SENSITIVITY_OFFSET;
        collationId |= accentSensitivity.ordinal() << ACCENT_SENSITIVITY_OFFSET;
        collationId |= caseConversion.ordinal() << CASE_CONVERSION_OFFSET;
        collationId |= ICULocaleToId.get(locale);
        return collationId;
      }

      @Override
      protected Collation buildCollation() {
        ULocale.Builder builder = new ULocale.Builder();
        builder.setLocale(ICULocaleMap.get(locale));
        if (caseSensitivity == CaseSensitivity.CS &&
            accentSensitivity == AccentSensitivity.AS) {
          builder.setUnicodeLocaleKeyword("ks", "level3");
        } else if (caseSensitivity == CaseSensitivity.CS &&
            accentSensitivity == AccentSensitivity.AI) {
          builder
            .setUnicodeLocaleKeyword("ks", "level1")
            .setUnicodeLocaleKeyword("kc", "true");
        } else if (caseSensitivity == CaseSensitivity.CI &&
            accentSensitivity == AccentSensitivity.AS) {
          builder.setUnicodeLocaleKeyword("ks", "level2");
        } else if (caseSensitivity == CaseSensitivity.CI &&
            accentSensitivity == AccentSensitivity.AI) {
          builder.setUnicodeLocaleKeyword("ks", "level1");
        }
        ULocale resultLocale = builder.build();
        Collator collator = Collator.getInstance(resultLocale);
        Comparator<UTF8String> comparator = (s1, s2) -> collator.compare(
          caseConversion(resultLocale, s1), caseConversion(resultLocale, s2));
        collator.freeze();
        return new Collation(
          collationName(),
          collator,
          comparator,
          "153.120.0.0",
          s -> (long) collator.getCollationKey(s.toString()).hashCode(),
          collationId == UNICODE_COLLATION_ID,
          false,
          false);
      }

      private String caseConversion(ULocale locale, UTF8String s) {
        if (caseConversion == CaseConversion.LCASE) {
          return UCharacter.toLowerCase(locale, s.toString());
        } else if (caseConversion == CaseConversion.UCASE) {
          return UCharacter.toUpperCase(locale, s.toString());
        } else {
          return s.toString();
        }
      }

      private String collationName() {
        StringBuilder builder = new StringBuilder();
        if (locale.equals("UNICODE")) {
          builder.append("UNICODE");
        } else {
          ULocale uLocale = ICULocaleMap.get(locale);
          builder.append(uLocale.getLanguage());
          String script = uLocale.getScript();
          if (!script.isEmpty()) {
            builder.append('_');
            builder.append(script);
          }
          String country = uLocale.getISO3Country();
          if (!country.isEmpty()) {
            builder.append('_');
            builder.append(country);
          }
        }
        if (caseSensitivity != CaseSensitivity.CS) {
          builder.append('_');
          builder.append(caseSensitivity.toString());
        }
        if (accentSensitivity != AccentSensitivity.AS) {
          builder.append('_');
          builder.append(accentSensitivity.toString());
        }
        if (caseConversion != CaseConversion.UNSPECIFIED) {
          builder.append('_');
          builder.append(caseConversion.toString());
        }
        return builder.toString();
      }
    }
  }

  public static final int UTF8_BINARY_COLLATION_ID =
    Collation.CollationSpecUTF8Binary.UTF8_BINARY_COLLATION_ID;
  public static final int UTF8_BINARY_LCASE_COLLATION_ID =
    Collation.CollationSpecUTF8Binary.UTF8_BINARY_LCASE_COLLATION_ID;
  public static final int UNICODE_COLLATION_ID =
    Collation.CollationSpecICU.UNICODE_COLLATION_ID;
  public static final int UNICODE_CI_COLLATION_ID =
    Collation.CollationSpecICU.UNICODE_CI_COLLATION_ID;
  public static final int INDETERMINATE_COLLATION_ID =
    Collation.CollationSpec.INDETERMINATE_COLLATION_ID;

  /**
   * Returns a StringSearch object for the given pattern and target strings, under collation
   * rules corresponding to the given collationId. The external ICU library StringSearch object can
   * be used to find occurrences of the pattern in the target string, while respecting collation.
   */
  public static StringSearch getStringSearch(
      final UTF8String targetUTF8String,
      final UTF8String patternUTF8String,
      final int collationId) {
    return getStringSearch(targetUTF8String.toString(), patternUTF8String.toString(), collationId);
  }

  /**
   * Returns a StringSearch object for the given pattern and target strings, under collation
   * rules corresponding to the given collationId. The external ICU library StringSearch object can
   * be used to find occurrences of the pattern in the target string, while respecting collation.
   */
  public static StringSearch getStringSearch(
          final String targetString,
          final String patternString,
          final int collationId) {
    CharacterIterator target = new StringCharacterIterator(targetString);
    Collator collator = CollationFactory.fetchCollation(collationId).collator;
    return new StringSearch(patternString, target, (RuleBasedCollator) collator);
  }

  /**
   * Returns if the given collationName is valid one.
   */
  public static boolean isValidCollation(String collationName) {
    try {
      fetchCollation(collationName);
      return true;
    } catch (SparkException e) {
      return false;
    }
  }

  /**
   * Returns a collation-unaware StringSearch object for the given pattern and target strings.
   * While this object does not respect collation, it can be used to find occurrences of the pattern
   * in the target string for UTF8_BINARY or UTF8_BINARY_LCASE (if arguments are lowercased).
   */
  public static StringSearch getStringSearch(
          final UTF8String targetUTF8String,
          final UTF8String patternUTF8String) {
    return new StringSearch(patternUTF8String.toString(), targetUTF8String.toString());
  }

  /**
   * Returns the collation id for the given collation name.
   */
  public static int collationNameToId(String collationName) throws SparkException {
    return Collation.CollationSpec.collationNameToId(collationName);
  }

  public static Collation fetchCollationUnsafe(int collationId) throws SparkException {
    return Collation.CollationSpec.fetchCollation(collationId);
  }

  public static Collation fetchCollation(int collationId) {
    try {
      return fetchCollationUnsafe(collationId);
    } catch (SparkException e) {
      return Collation.CollationSpecUTF8Binary.UTF8_BINARY_COLLATION;
    }
  }

  public static Collation fetchCollation(String collationName) throws SparkException {
    return fetchCollation(collationNameToId(collationName));
  }

  public static String[] getICULocaleNames() {
    return Collation.CollationSpecICU.ICULocaleNames;
  }
}
