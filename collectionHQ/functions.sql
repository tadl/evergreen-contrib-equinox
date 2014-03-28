--  
--   Functions used by collectionHQ reporting.  This only needs to be run once.
--  

--    Copyright (C) 2011-2012 Equinox Software Inc.
--    Ben Ostrowsky <ben@esilibrary.com>
--    Galen Charlton <gmc@esilibrary.com>
--
--    Original version sponsored by the King County Library System
--
--    This program is free software; you can redistribute it and/or modify
--    it under the terms of the GNU General Public License as published by
--    the Free Software Foundation; either version 2 of the License, or
--    (at your option) any later version.
--
--    This program is distributed in the hope that it will be useful,
--    but WITHOUT ANY WARRANTY; without even the implied warranty of
--    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
--    GNU General Public License for more details.
--
--    You should have received a copy of the GNU General Public License
--    along with this program; if not, write to the Free Software
--    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

CREATE SCHEMA collectionHQ;

CREATE OR REPLACE FUNCTION collectionHQ.attempt_year (TEXT) RETURNS TEXT AS $$
  DECLARE
    attempt_value ALIAS FOR $1;
    output TEXT;
  BEGIN
    FOR output IN
      EXECUTE 'SELECT SUBSTRING(REGEXP_REPLACE(' || quote_literal(attempt_value) || E', E\'[^0-9]\', \'\', \'g\') FROM 1 FOR 4) AS a;'
    LOOP
      RETURN output;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      FOR output IN
        EXECUTE E'SELECT \'\' AS a;'
      LOOP
        RETURN output;
      END LOOP;
  END;
$$ LANGUAGE PLPGSQL STRICT STABLE;


CREATE OR REPLACE FUNCTION collectionHQ.attempt_price (TEXT) RETURNS TEXT AS $$
  DECLARE
    attempt_value ALIAS FOR $1;
    output TEXT;
  BEGIN
    FOR output IN
      EXECUTE 'SELECT (REGEXP_REPLACE(' || quote_literal(attempt_value) || E', E\'[^0-9\.]\', \'\', \'g\')::NUMERIC(8,2) * 100)::INTEGER AS a;'
    LOOP
      RETURN output;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      FOR output IN
        EXECUTE E'SELECT \'\' AS a;'
      LOOP
        RETURN output;
      END LOOP;
  END;
$$ LANGUAGE PLPGSQL STRICT STABLE;


CREATE OR REPLACE FUNCTION collectionHQ.quote (TEXT) RETURNS TEXT AS $$
  DECLARE
    value ALIAS FOR $1;
    output TEXT;
  BEGIN
    IF value IS NOT NULL AND value <> '' THEN
      RETURN '##' || value || '##';
    ELSE
      RETURN '';
    END IF;
  END;
$$ LANGUAGE PLPGSQL STRICT STABLE;

DROP FUNCTION IF EXISTS collectionHQ.write_item_rows_to_stdout (TEXT, INT);
CREATE OR REPLACE FUNCTION collectionHQ.write_item_rows_to_stdout (TEXT, INT) RETURNS VOID AS $$
-- Usage: SELECT collectionHQ.write_item_rows_to_stdout ('LIBRARYCODE',org_unit_id);

  DECLARE
    item BIGINT;
    authority_code ALIAS FOR $1;
    org_unit_id ALIAS for $2;
    lms_bib_id BIGINT;
    library_code TEXT;
    bar_code TEXT;
    last_use_date TEXT;
    cumulative_use_total TEXT;
    cumulative_use_current TEXT;
    status TEXT;
    date_added TEXT;
    price TEXT;
    purchase_code TEXT;
    rotating_stock TEXT;
    lib_supsel_tag TEXT;
    gen_supsel_tag TEXT;
    notes TEXT;
    extract_date TEXT;
    collection_code TEXT;
    collection_code_level_2 TEXT;
    filter_level_1 TEXT;
    filter_level_2 TEXT;
    filter_level_3 TEXT;
    filter_level_4 TEXT;
    isbn TEXT := '';
    output TEXT := '';
    arrived TIMESTAMPTZ;
    num_rows INTEGER := 0;

  BEGIN

    FOR item IN
      SELECT id FROM asset.copy WHERE NOT deleted AND circ_lib IN (SELECT id FROM actor.org_unit_descendants(org_unit_id)) ORDER BY id
    LOOP

      SELECT cn.record, cn.label
      INTO lms_bib_id, filter_level_1
      FROM asset.call_number cn, asset.copy c 
      WHERE c.call_number = cn.id AND c.id =  item;
      SELECT r.isbn[1] INTO isbn
      FROM reporter.materialized_simple_record r
      WHERE id = lms_bib_id;
      SELECT collectionHQ.attempt_price(ac.price::TEXT), barcode, ac.status,
             REPLACE(create_date::DATE::TEXT, '-', ''),
             CASE WHEN floating::INT > 0 THEN 'Y' ELSE NULL END
      INTO price, bar_code, status, date_added, rotating_stock
      FROM asset.copy ac 
      WHERE id = item;
      IF price IS NULL OR price = '' THEN
        SELECT collectionHQ.attempt_price((XPATH('//marc:datafield[@tag="020"][1]/marc:subfield[@code="c"]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']]))[1]::TEXT)
        INTO price
        FROM biblio.record_entry
        WHERE id = lms_bib_id;
      END IF;
      SELECT REPLACE(NOW()::DATE::TEXT, '-', '') INTO extract_date;
      SELECT ou.shortname INTO library_code FROM actor.org_unit ou, asset.copy c WHERE ou.id = c.circ_lib AND c.id = item;
      SELECT REPLACE(xact_start::DATE::TEXT, '-', '') INTO last_use_date FROM action.circulation WHERE target_copy = item ORDER BY xact_start DESC LIMIT 1;
      SELECT circ_count INTO cumulative_use_total FROM extend_reporter.full_circ_count WHERE id = item;
      IF cumulative_use_total IS NULL THEN
        cumulative_use_total := '0';
      END IF;
      SELECT MAX(dest_recv_time) INTO arrived
      FROM action.transit_copy atc
      JOIN asset.copy ac ON (ac.id = atc.target_copy AND ac.circ_lib = atc.dest)
      WHERE ac.id = item;
      IF arrived IS NOT NULL THEN
        SELECT COUNT(*) INTO cumulative_use_current FROM action.circulation WHERE target_copy = item AND xact_start > arrived;
      ELSE
      cumulative_use_current := '0'; 
      END IF;
      SELECT SUBSTRING(value FROM 1 FOR 100) INTO notes FROM asset.copy_note WHERE owning_copy = item AND title ILIKE '%collectionHQ%' ORDER BY id LIMIT 1;
      SELECT l.name INTO collection_code FROM asset.copy c, asset.copy_location l WHERE c.location = l.id AND c.id = item;
  
      purchase_code := ''; -- FIXME do we want something else here?
      lib_supsel_tag := ''; -- FIXME do we want something else here?
      gen_supsel_tag := ''; -- FIXME do we want something else here?
      collection_code_level_2 := ''; -- FIXME do we want something else here?
      filter_level_2 := ''; -- FIXME do we want something else here?
      filter_level_3 := ''; -- FIXME do we want something else here?
      filter_level_4 := ''; -- FIXME do we want something else here?
  
      output := '##HOLD##,'
        || lms_bib_id || ','
        || COALESCE(collectionHQ.quote(authority_code), '') || ','
        || COALESCE(collectionHQ.quote(library_code), '') || ','
        || COALESCE(collectionHQ.quote(bar_code), '') || ','
        || COALESCE(collectionHQ.quote(last_use_date), '') || ','
        || COALESCE(cumulative_use_total, '') || ','
        || COALESCE(cumulative_use_current, '') || ','
        || COALESCE(collectionHQ.quote(status), '') || ','
        || COALESCE(collectionHQ.quote(date_added), '') || ','
        || COALESCE(price, '') || ','
        || COALESCE(collectionHQ.quote(purchase_code), '') || ','
        || COALESCE(collectionHQ.quote(rotating_stock), '') || ','
        || COALESCE(collectionHQ.quote(lib_supsel_tag), '') || ','
        || COALESCE(collectionHQ.quote(gen_supsel_tag), '') || ','
        || COALESCE(collectionHQ.quote(notes), '') || ','
        || COALESCE(collectionHQ.quote(extract_date), '') || ','
        || COALESCE(collectionHQ.quote(collection_code), '') || ','
        || COALESCE(collectionHQ.quote(collection_code_level_2), '') || ','
        || COALESCE(collectionHQ.quote(filter_level_1), '') || ','
        || COALESCE(collectionHQ.quote(filter_level_2), '') || ','
        || COALESCE(collectionHQ.quote(filter_level_3), '') || ','
        || COALESCE(collectionHQ.quote(filter_level_4), '') || ','
        || COALESCE(collectionHQ.quote(isbn), '');
  
       RAISE INFO '%', output;

       num_rows := num_rows + 1;
       IF (num_rows::numeric % 1000.0 = 0.0) THEN RAISE INFO '% rows written', num_rows; END IF;

    END LOOP;

    RAISE INFO '% rows written in total.', num_rows;

  END;

$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS collectionHQ.write_bib_rows_to_stdout(TEXT, INT);
CREATE OR REPLACE FUNCTION collectionHQ.write_bib_rows_to_stdout (TEXT, INT) RETURNS VOID AS $$
-- Usage: SELECT collectionHQ.write_bib_rows_to_stdout('LIBRARYCODE', org_unit_id);

  DECLARE
    library_service_code ALIAS FOR $1;
    org_unit_id ALIAS FOR $2;
    isbn TEXT := '';
    title TEXT := '';
    author TEXT := '';
    edition_num TEXT := '';
    publication_date TEXT := '';
    publisher TEXT := '';
    price TEXT := '';
    lms_item_type TEXT := '';
    class_num TEXT := '';
    extract_date TEXT := '';
    output TEXT := '';
    lms_bib_id BIGINT;
    num_rows INTEGER := 0;

  BEGIN

    FOR lms_bib_id IN
      SELECT DISTINCT bre.id FROM biblio.record_entry bre JOIN asset.call_number acn ON (acn.record = bre.id) WHERE acn.owning_lib IN (SELECT id FROM actor.org_unit_descendants(org_unit_id)) AND NOT acn.deleted AND NOT bre.deleted
    LOOP

      SELECT r.isbn[1],
             SUBSTRING(r.title FROM 1 FOR 100),
             SUBSTRING(r.author FROM 1 FOR 50)
      INTO isbn, title, author
      FROM reporter.materialized_simple_record r
      WHERE id = lms_bib_id;
      SELECT 
        SUBSTRING(naco_normalize((XPATH('//marc:datafield[@tag="250"][1]/marc:subfield[@code="a"]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']]))[1]::TEXT, 'a') FROM 1 FOR 20),
        collectionHQ.attempt_year((XPATH('//marc:datafield[@tag="260"][1]/marc:subfield[@code="c"]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']]))[1]::TEXT),
        SUBSTRING(naco_normalize((XPATH('//marc:datafield[@tag="260"][1]/marc:subfield[@code="b"]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']]))[1]::TEXT, 'b') FROM 1 FOR 100),
        collectionHQ.attempt_price((XPATH('//marc:datafield[@tag="020"][1]/marc:subfield[@code="c"]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']]))[1]::TEXT),
        SUBSTRING(naco_normalize((XPATH('//marc:datafield[@tag="082"][1]/marc:subfield[@code="a"][1]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']]))[1]::TEXT, 'a') FROM 1 FOR 20)
      INTO edition_num, publication_date, publisher, price, class_num
      FROM biblio.record_entry
      WHERE id = lms_bib_id;

      SELECT circ_modifier INTO lms_item_type FROM asset.copy c, asset.call_number cn WHERE cn.record = lms_bib_id AND c.circ_lib IN (SELECT id FROM actor.org_unit_descendants(org_unit_id)) AND cn.id = c.call_number AND NOT cn.deleted AND NOT c.deleted LIMIT 1;
      SELECT REPLACE(NOW()::DATE::TEXT, '-', '') INTO extract_date;
  
      output := 
        '##BIB##,'
        || lms_bib_id || ','
        || COALESCE(collectionHQ.quote(library_service_code), '') || ','
        || COALESCE(collectionHQ.quote(isbn), '') || ','
        || COALESCE(collectionHQ.quote(title), '') || ','
        || COALESCE(collectionHQ.quote(author), '') || ','
        || COALESCE(collectionHQ.quote(edition_num), '') || ','
        || COALESCE(collectionHQ.quote(publication_date), '') || ','
        || COALESCE(collectionHQ.quote(publisher), '') || ','
        || COALESCE(price, '') || ','
        || COALESCE(collectionHQ.quote(lms_item_type), '') || ','
        || COALESCE(collectionHQ.quote(class_num), '') || ','
        || COALESCE(collectionHQ.quote(extract_date), '');
  
       RAISE INFO '%', output;

       num_rows := num_rows + 1;
       IF (num_rows::numeric % 1000.0 = 0.0) THEN RAISE INFO '% rows written', num_rows; END IF;

    END LOOP;

    RAISE INFO '% rows written in total.', num_rows;

  END;

$$ LANGUAGE plpgsql;
