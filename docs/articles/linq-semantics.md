# LINQ Semantics Contract

This page is the normative contract of the LINQ compiler. It defines what every node of the query IR means, so that
a query returns the same rows on any backend and an optimizer pass knows what it may rely on. The text below is
included from the source file `kernel/Samidare/IR/SEMANTICS.md`, and the IR conformance test suite executes each rule.

For the same rules from the point of view of someone writing queries, with examples, see
[Query Semantics](/articles/linq#_8-query-semantics-matching-linq-to-objects).

<!--@include: ../../kernel/Samidare/IR/SEMANTICS.md{3,}-->
