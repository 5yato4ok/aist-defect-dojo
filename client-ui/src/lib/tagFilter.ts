export type TagMatchMode = "any" | "all";

export type TagFilterState = "none" | "include" | "exclude";

/**
 * Tri-state tag filter. ``include`` narrows to findings carrying the tags
 * (``matchMode`` decides any-of vs all-of), ``exclude`` drops findings carrying
 * any of its tags. A tag never sits in both lists.
 */
export type TagFilterValue = {
  include: string[];
  exclude: string[];
  matchMode: TagMatchMode;
};

export const EMPTY_TAG_FILTER: TagFilterValue = { include: [], exclude: [], matchMode: "any" };

export function normalizeTagList(tags: readonly string[]): string[] {
  return [...new Set(tags.map((tag) => tag.trim()).filter(Boolean))]
    .sort((left, right) => left.localeCompare(right));
}

export function normalizeTagFilter(value: Partial<TagFilterValue>): TagFilterValue {
  const include = normalizeTagList(value.include ?? []);
  const exclude = normalizeTagList(value.exclude ?? []).filter((tag) => !include.includes(tag));
  return { include, exclude, matchMode: value.matchMode === "all" ? "all" : "any" };
}

export function tagFilterState(value: TagFilterValue, tag: string): TagFilterState {
  if (value.include.includes(tag)) return "include";
  if (value.exclude.includes(tag)) return "exclude";
  return "none";
}

export function tagFilterSize(value: TagFilterValue): number {
  return value.include.length + value.exclude.length;
}

export function hasTagFilter(value: TagFilterValue): boolean {
  return tagFilterSize(value) > 0;
}

export function isSameTagFilter(left: TagFilterValue, right: TagFilterValue): boolean {
  return (
    left.matchMode === right.matchMode
    && left.include.length === right.include.length
    && left.exclude.length === right.exclude.length
    && left.include.every((tag, index) => tag === right.include[index])
    && left.exclude.every((tag, index) => tag === right.exclude[index])
  );
}

export function setTagState(value: TagFilterValue, tag: string, state: TagFilterState): TagFilterValue {
  return normalizeTagFilter({
    matchMode: value.matchMode,
    include: state === "include" ? [...value.include, tag] : value.include.filter((item) => item !== tag),
    exclude: state === "exclude" ? [...value.exclude, tag] : value.exclude.filter((item) => item !== tag),
  });
}

/** Click on a tag name: none → include → exclude → none. */
export function cycleTagState(value: TagFilterValue, tag: string): TagFilterValue {
  const current = tagFilterState(value, tag);
  const next: TagFilterState = current === "none" ? "include" : current === "include" ? "exclude" : "none";
  return setTagState(value, tag, next);
}

export function toggleTagState(value: TagFilterValue, tag: string, state: Exclude<TagFilterState, "none">): TagFilterValue {
  return setTagState(value, tag, tagFilterState(value, tag) === state ? "none" : state);
}

export function setTagMatchMode(value: TagFilterValue, matchMode: TagMatchMode): TagFilterValue {
  return matchMode === value.matchMode ? value : { ...value, matchMode };
}

/** Keep only tags that still exist in the option list (e.g. after a project switch). */
export function pruneTagFilter(value: TagFilterValue, available: readonly string[]): TagFilterValue {
  const allowed = new Set(available);
  const include = value.include.filter((tag) => allowed.has(tag));
  const exclude = value.exclude.filter((tag) => allowed.has(tag));
  if (include.length === value.include.length && exclude.length === value.exclude.length) return value;
  return { ...value, include, exclude };
}

/** Replace the whole selection with a single included tag; keeps the match mode. */
export function onlyTag(value: TagFilterValue, tag: string): TagFilterValue {
  return normalizeTagFilter({ include: [tag], exclude: [], matchMode: value.matchMode });
}

/** Client-side counterpart of the API semantics: none of ``exclude``, any/all of ``include``. */
export function matchesTagFilter(tags: readonly string[], value: TagFilterValue): boolean {
  if (value.exclude.some((tag) => tags.includes(tag))) return false;
  if (value.include.length === 0) return true;
  return value.matchMode === "all"
    ? value.include.every((tag) => tags.includes(tag))
    : value.include.some((tag) => tags.includes(tag));
}

export function describeTagFilter(value: TagFilterValue): string {
  const parts: string[] = [];
  if (value.include.length) {
    parts.push(`${value.matchMode === "all" ? "All of" : "Any of"} ${value.include.join(", ")}`);
  }
  if (value.exclude.length) {
    parts.push(`not ${value.exclude.join(", ")}`);
  }
  return parts.join(" · ");
}
