--
-- PostgreSQL database dump
--

-- Dumped from database version 16.4
-- Dumped by pg_dump version 16.6 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: bitcoin_balances; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitcoin_balances (
    address text NOT NULL,
    total_input numeric DEFAULT 0,
    total_output numeric DEFAULT 0,
    balance numeric DEFAULT 0
);


ALTER TABLE public.bitcoin_balances OWNER TO postgres;

--
-- Name: bitcoin_transactions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bitcoin_transactions (
    id integer NOT NULL,
    hash text NOT NULL,
    address text NOT NULL,
    category text NOT NULL,
    value numeric NOT NULL,
    "timestamp" timestamp without time zone NOT NULL
);


ALTER TABLE public.bitcoin_transactions OWNER TO postgres;

--
-- Name: bitcoin_transactions_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.bitcoin_transactions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.bitcoin_transactions_id_seq OWNER TO postgres;

--
-- Name: bitcoin_transactions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.bitcoin_transactions_id_seq OWNED BY public.bitcoin_transactions.id;


--
-- Name: bitcoin_transactions id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bitcoin_transactions ALTER COLUMN id SET DEFAULT nextval('public.bitcoin_transactions_id_seq'::regclass);


--
-- Name: bitcoin_balances bitcoin_balances_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bitcoin_balances
    ADD CONSTRAINT bitcoin_balances_pkey PRIMARY KEY (address);


--
-- Name: bitcoin_transactions bitcoin_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bitcoin_transactions
    ADD CONSTRAINT bitcoin_transactions_pkey PRIMARY KEY (id);


--
-- Name: unique_transaction_entry; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX unique_transaction_entry ON public.bitcoin_transactions USING btree (hash, category, address, value, "timestamp");


--
-- PostgreSQL database dump complete
--

