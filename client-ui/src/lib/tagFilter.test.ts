import { describe, expect, it } from "vitest";

import {
  EMPTY_TAG_FILTER,
  cycleTagState,
  describeTagFilter,
  isSameTagFilter,
  matchesTagFilter,
  normalizeTagFilter,
  onlyTag,
  pruneTagFilter,
  setTagMatchMode,
  setTagState,
  tagFilterState,
  toggleTagState,
} from "./tagFilter";

describe("tagFilter", () => {
  it("cycles a tag through include, exclude and back to neutral", () => {
    const included = cycleTagState(EMPTY_TAG_FILTER, "dast");
    expect(tagFilterState(included, "dast")).toBe("include");

    const excluded = cycleTagState(included, "dast");
    expect(tagFilterState(excluded, "dast")).toBe("exclude");
    expect(excluded.include).toEqual([]);

    const cleared = cycleTagState(excluded, "dast");
    expect(tagFilterState(cleared, "dast")).toBe("none");
  });

  it("never keeps a tag in both lists", () => {
    const value = setTagState(setTagState(EMPTY_TAG_FILTER, "dast", "include"), "dast", "exclude");
    expect(value).toEqual({ include: [], exclude: ["dast"], matchMode: "any" });

    const normalized = normalizeTagFilter({ include: ["a", "b"], exclude: ["b", "c"] });
    expect(normalized).toEqual({ include: ["a", "b"], exclude: ["c"], matchMode: "any" });
  });

  it("supports the 'dast but not inconclusive' scenario", () => {
    let value = toggleTagState(EMPTY_TAG_FILTER, "dast", "include");
    value = toggleTagState(value, "inconclusive", "exclude");
    expect(value).toEqual({ include: ["dast"], exclude: ["inconclusive"], matchMode: "any" });
    expect(describeTagFilter(value)).toBe("Any of dast · not inconclusive");

    // toggling the same state again releases the tag
    expect(toggleTagState(value, "inconclusive", "exclude").exclude).toEqual([]);
  });

  it("normalizes whitespace, duplicates and order", () => {
    expect(normalizeTagFilter({ include: [" b ", "a", "b", ""], exclude: ["  "], matchMode: "all" }))
      .toEqual({ include: ["a", "b"], exclude: [], matchMode: "all" });
    expect(describeTagFilter({ include: ["a", "b"], exclude: [], matchMode: "all" })).toBe("All of a, b");
  });

  it("switches match mode without touching the lists", () => {
    const value = { include: ["a"], exclude: ["b"], matchMode: "any" as const };
    expect(setTagMatchMode(value, "all")).toEqual({ ...value, matchMode: "all" });
    expect(setTagMatchMode(value, "any")).toBe(value);
  });

  it("prunes tags that disappeared from the option list and keeps identity otherwise", () => {
    const value = { include: ["dast", "gone"], exclude: ["inconclusive", "stale"], matchMode: "any" as const };
    expect(pruneTagFilter(value, ["dast", "inconclusive"]))
      .toEqual({ include: ["dast"], exclude: ["inconclusive"], matchMode: "any" });
    const stable = { include: ["dast"], exclude: [], matchMode: "any" as const };
    expect(pruneTagFilter(stable, ["dast", "other"])).toBe(stable);
  });

  it("matches item tag lists with the same semantics as the API", () => {
    const anyOf = { include: ["dast", "sast"], exclude: ["inconclusive"], matchMode: "any" as const };
    expect(matchesTagFilter(["dast"], anyOf)).toBe(true);
    expect(matchesTagFilter(["dast", "inconclusive"], anyOf)).toBe(false);
    expect(matchesTagFilter(["other"], anyOf)).toBe(false);

    const allOf = { ...anyOf, matchMode: "all" as const };
    expect(matchesTagFilter(["dast"], allOf)).toBe(false);
    expect(matchesTagFilter(["dast", "sast", "x"], allOf)).toBe(true);

    expect(matchesTagFilter([], EMPTY_TAG_FILTER)).toBe(true);
    expect(matchesTagFilter(["a"], { include: [], exclude: ["a"], matchMode: "any" })).toBe(false);
  });

  it("collapses the selection to a single tag with onlyTag", () => {
    const value = { include: ["a", "b"], exclude: ["c"], matchMode: "all" as const };
    expect(onlyTag(value, "dast")).toEqual({ include: ["dast"], exclude: [], matchMode: "all" });
  });

  it("compares filters structurally", () => {
    expect(isSameTagFilter(
      { include: ["a"], exclude: ["b"], matchMode: "any" },
      { include: ["a"], exclude: ["b"], matchMode: "any" },
    )).toBe(true);
    expect(isSameTagFilter(
      { include: ["a"], exclude: ["b"], matchMode: "any" },
      { include: ["a"], exclude: ["b"], matchMode: "all" },
    )).toBe(false);
  });
});
