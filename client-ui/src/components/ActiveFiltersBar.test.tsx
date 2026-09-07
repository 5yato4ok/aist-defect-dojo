// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { ActiveFilterChip } from "../lib/findingsActiveFilters";
import ActiveFiltersBar from "./ActiveFiltersBar";

const CHIPS: ActiveFilterChip[] = [
  { id: "status", label: "Status", value: "Active", tone: "neutral", patch: { status: "All" } },
  { id: "tag:include:dast", label: "Tag", value: "dast", tone: "include", patch: {} },
  { id: "tag:exclude:inconclusive", label: "Not tag", value: "inconclusive", tone: "exclude", patch: {} },
];

afterEach(() => cleanup());

describe("ActiveFiltersBar", () => {
  it("renders nothing when no filter is active", () => {
    const { container } = render(<ActiveFiltersBar chips={[]} onRemove={() => {}} onClearAll={() => {}} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("lists every active filter and hands the removed chip back", () => {
    const onRemove = vi.fn();
    const onClearAll = vi.fn();
    render(<ActiveFiltersBar chips={CHIPS} onRemove={onRemove} onClearAll={onClearAll} />);

    expect(screen.getByLabelText("Active filters")).toHaveTextContent("Status");
    expect(screen.getByText("inconclusive")).toHaveClass("line-through");

    fireEvent.click(screen.getByRole("button", { name: "Remove filter Not tag inconclusive" }));
    expect(onRemove).toHaveBeenCalledWith(CHIPS[2]);

    fireEvent.click(screen.getByRole("button", { name: "Clear all" }));
    expect(onClearAll).toHaveBeenCalledTimes(1);
  });
});
