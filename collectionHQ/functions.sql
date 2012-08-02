--  
--   Functions used by collectionHQ reporting.  This only needs to be run once.
--  

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


CREATE OR REPLACE FUNCTION collectionHQ.write_item_rows_to_stdout (TEXT, INT) RETURNS TEXT AS $$
-- Usage: SELECT collectionHQ.write_item_rows_to_stdout ('LIBRARYCODE',org_unit_id);

  DECLARE
    item BIGINT;
    authority_code ALIAS FOR $1;
	org_unit_id ALIAS for $2;
    lms_bib_id TEXT;
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
      EXECUTE ('SELECT id FROM asset.copy WHERE NOT deleted AND circ_lib IN (SELECT id FROM actor.org_unit_descendants(' || org_unit_id || ')) ORDER BY id;')
    LOOP

      EXECUTE ('SELECT cn.record FROM asset.call_number cn, asset.copy c WHERE c.call_number = cn.id AND c.id = ' || item || ';') INTO lms_bib_id;
      EXECUTE (E'SELECT isbn[1] FROM reporter.materialized_simple_record r WHERE r.id = ' || lms_bib_id || ';') INTO isbn;
      EXECUTE ('SELECT collectionHQ.attempt_price(price::TEXT) FROM asset.copy WHERE id = ' || item || ';') INTO price;
      IF price IS NULL OR price = '' THEN
        EXECUTE (E'SELECT collectionHQ.attempt_price(value) FROM metabib.real_full_rec WHERE record = ' || lms_bib_id || E' AND tag = \'020\' AND subfield = \'c\' LIMIT 1;') INTO price;
      END IF;
      EXECUTE (E'SELECT REPLACE(NOW()::DATE::TEXT, \'-\', \'\');') INTO extract_date;
      EXECUTE ('SELECT ou.shortname FROM actor.org_unit ou, asset.copy c WHERE ou.id = c.circ_lib AND c.id = ' || item || ';') INTO library_code;
      EXECUTE ('SELECT barcode FROM asset.copy WHERE id = ' || item || ';') INTO bar_code;
      EXECUTE (E'SELECT REPLACE(xact_start::DATE::TEXT, \'-\', \'\') FROM action.circulation WHERE target_copy = ' || item || ' ORDER BY xact_start DESC LIMIT 1;') INTO last_use_date;
      EXECUTE (E'SELECT circ_count FROM extend_reporter.full_circ_count WHERE id = ' || item || ';') INTO cumulative_use_total;
      IF cumulative_use_total IS NULL THEN
        cumulative_use_total := '0';
      END IF;
      EXECUTE ('SELECT h.audit_time FROM asset.copy c, auditor.asset_copy_history h WHERE c.id = h.id AND c.circ_lib <> h.circ_lib AND c.id = ' || item || 'ORDER BY h.audit_time DESC LIMIT 1;') INTO arrived;
      IF arrived IS NOT NULL THEN
        EXECUTE('SELECT COUNT(*) FROM action.circulation WHERE target_copy = ' || item || ' AND xact_start > ' || quote_literal(arrived) || '::TIMESTAMPTZ;') INTO cumulative_use_current;
      ELSE
      cumulative_use_current := '0'; 
      END IF;
      EXECUTE ('SELECT status FROM asset.copy WHERE id = ' || item || ';') INTO status;
      EXECUTE (E'SELECT REPLACE(create_date::DATE::TEXT, \'-\', \'\') FROM asset.copy WHERE id = ' || item || ';') INTO date_added;
      EXECUTE (E'SELECT CASE floating WHEN TRUE THEN \'Y\' ELSE NULL END FROM asset.copy WHERE id = ' || item || ';') INTO rotating_stock;
      EXECUTE ('SELECT SUBSTRING(value FROM 1 FOR 100) FROM asset.copy_note WHERE owning_copy = ' || item || E' AND title ILIKE \'%collectionHQ%\' ORDER BY id LIMIT 1;') INTO notes; -- FIXME or we could compile and concatenate in another function
      EXECUTE ('SELECT l.name FROM asset.copy c, asset.copy_location l WHERE c.location = l.id AND c.id = ' || item || ';') INTO collection_code;
      EXECUTE ('SELECT v.label FROM asset.call_number v, asset.copy c WHERE v.id = c.call_number AND c.id = ' || item || ';') INTO filter_level_1;
  
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
       IF (num_rows::numeric % 10000.0 = 0.0) THEN RAISE INFO '% rows written', num_rows; END IF;

    END LOOP;

    RAISE INFO '% rows written in total.', num_rows;

  END;

$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION collectionHQ.write_bib_rows_to_stdout (TEXT, INT) RETURNS TEXT AS $$
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
      EXECUTE ('SELECT bre.id FROM biblio.record_entry bre JOIN asset.call_number acn ON (acn.record = bre.id) WHERE acn.owning_lib IN (SELECT id FROM actor.org_unit_descendants(' || org_unit_id || ')) AND NOT acn.deleted;')
    LOOP

      EXECUTE (E'SELECT isbn[1] FROM reporter.materialized_simple_record r WHERE r.id = ' || lms_bib_id || ';') INTO isbn;
      EXECUTE ('SELECT SUBSTRING(title FROM 1 FOR 100) FROM reporter.materialized_simple_record r WHERE r.id = ' || lms_bib_id || ';') INTO title;
      EXECUTE ('SELECT SUBSTRING(author FROM 1 FOR 50) FROM reporter.materialized_simple_record r WHERE r.id = ' || lms_bib_id || ';') INTO author;
      EXECUTE (E'SELECT SUBSTRING(value FROM 1 FOR 20) FROM metabib.real_full_rec WHERE record = ' || lms_bib_id || E' AND tag = \'250\' AND subfield = \'a\' LIMIT 1;') INTO edition_num;
      EXECUTE (E'SELECT collectionHQ.attempt_year(value) FROM metabib.real_full_rec WHERE record = ' || lms_bib_id || E' AND tag = \'260\' AND subfield = \'c\' LIMIT 1;') INTO publication_date;
      EXECUTE (E'SELECT SUBSTRING(value FROM 1 FOR 100) FROM metabib.real_full_rec WHERE record = ' || lms_bib_id || E' AND tag = \'260\' AND subfield = \'b\' LIMIT 1;') INTO publisher;
      EXECUTE (E'SELECT collectionHQ.attempt_price(value) FROM metabib.real_full_rec WHERE record = ' || lms_bib_id || E' AND tag = \'020\' AND subfield = \'c\' LIMIT 1;') INTO price;
      EXECUTE ('SELECT circ_modifier FROM asset.copy c, asset.call_number cn WHERE cn.record = ' || lms_bib_id || ' AND cn.id = c.call_number AND NOT cn.deleted AND NOT c.deleted LIMIT 1;') INTO lms_item_type;
      EXECUTE ('SELECT SUBSTRING(value FROM 1 FOR 20) FROM metabib.real_full_rec WHERE record = ' || lms_bib_id || E' AND tag = \'082\' AND subfield = \'a\' LIMIT 1;') INTO class_num;
      EXECUTE (E'SELECT REPLACE(NOW()::DATE::TEXT, \'-\', \'\');') INTO extract_date;
  
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
       IF (num_rows::numeric % 10000.0 = 0.0) THEN RAISE INFO '% rows written', num_rows; END IF;

    END LOOP;

    RAISE INFO '% rows written in total.', num_rows;

  END;

$$ LANGUAGE plpgsql;
