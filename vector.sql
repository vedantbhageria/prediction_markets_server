-- The vector layer on top of schema.sql. Applied by store.ensure_schema()
-- only once the pgvector extension is enabled in this database (enabling it
-- needs a superuser, once:  sudo -u postgres psql -d prediction_markets
-- -c "CREATE EXTENSION vector;"); safe to re-run.
--
-- One embedding per distinct template -- every strike of a ladder shares its
-- market's question text, so they share one vector -- and an HNSW index, so
-- "the templates nearest this one" is answered in milliseconds over all of
-- them. Each market in `propositions` points here by its template text.

CREATE TABLE IF NOT EXISTS templates (
    template_id  BIGSERIAL PRIMARY KEY,
    template     TEXT NOT NULL UNIQUE,
    model        TEXT NOT NULL,
    embedding    vector(384) NOT NULL
);

CREATE INDEX IF NOT EXISTS templates_embedding_idx
    ON templates USING hnsw (embedding vector_cosine_ops);

-- Vectors made before pgvector was there (plain arrays) move across once.
DO $$
BEGIN
    IF to_regclass('template_vectors') IS NOT NULL THEN
        INSERT INTO templates (template, model, embedding)
        SELECT template, model, vec::vector FROM template_vectors
        ON CONFLICT (template) DO NOTHING;
        DROP TABLE template_vectors;
    END IF;
END $$;

-- The pre-computed pairs this replaces: matches are found when asked for.
DROP TABLE IF EXISTS matches;
