    CREATE TABLE IF NOT EXISTS apartments_property (
          url VARCHAR NOT NULL,
          site_last_mod VARCHAR,
          html_contents VARCHAR,
          last_downloaded VARCHAR,
          population BIGINT
          CONSTRAINT url_pk PRIMARY KEY (url));
