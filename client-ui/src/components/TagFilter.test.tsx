// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { useState } from "react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { EMPTY_TAG_FILTER, type TagFilterValue } from "../lib/tagFilter";
import TagFilter from "./TagFilter";

const OPTIONS = ["dast", "inconclusive", "cve", "aist:duplicate:candidate"];

function Harness({ initial = EMPTY_TAG_FILTER, onChange }: { initial?: TagFilterValue; onChange?: (value: TagFilterValue) => void }) {
  const [value, setValue] = useState<TagFilterValue>(initial);
  return (
    <TagFilter
      options={OPTIONS}
      value={value}
      onChange={(next) => {
        setValue(next);
        onChange?.(next);
      }}
    />
  );
}

function openPopover() {
  fireEvent.click(screen.getByRole("button", { name: /tags/i }));
  return screen.getByRole("listbox");
}

beforeAll(() => {
  // jsdom lacks the layout APIs cmdk relies on for keeping the highlighted row visible.
  Element.prototype.scrollIntoView = vi.fn();
  vi.stubGlobal("ResizeObserver", class {
    observe() {}
    unobserve() {}
    disconnect() {}
  });
});

afterEach(() => {
  cleanup();
});

describe("TagFilter", () => {
  it("shows a neutral summary and no chips when nothing is selected", () => {
    render(<Harness />);
    expect(screen.getByRole("button", { name: /tags/i })).toHaveTextContent("Any tag");
    expect(screen.queryByRole("button", { name: /remove/i })).not.toBeInTheDocument();
  });

  it("includes dast and excludes inconclusive through the explicit + / − controls", () => {
    const onChange = vi.fn();
    render(<Harness onChange={onChange} />);
    openPopover();

    fireEvent.click(screen.getByRole("button", { name: "Include dast" }));
    fireEvent.click(screen.getByRole("button", { name: "Exclude inconclusive" }));

    expect(onChange).toHaveBeenLastCalledWith({ include: ["dast"], exclude: ["inconclusive"], matchMode: "any" });
    expect(screen.getByRole("button", { name: /tags/i })).toHaveTextContent("Any of dast · not inconclusive");
    expect(screen.getByRole("button", { name: "Remove dast" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Remove inconclusive" })).toBeInTheDocument();
  });

  it("cycles a tag neutral → included → excluded → neutral when its row is clicked", () => {
    const onChange = vi.fn();
    render(<Harness onChange={onChange} />);
    const list = openPopover();
    const row = () => within(list).getByRole("option", { name: /^cve/ });

    fireEvent.click(row());
    expect(onChange).toHaveBeenLastCalledWith({ include: ["cve"], exclude: [], matchMode: "any" });
    fireEvent.click(row());
    expect(onChange).toHaveBeenLastCalledWith({ include: [], exclude: ["cve"], matchMode: "any" });
    fireEvent.click(row());
    expect(onChange).toHaveBeenLastCalledWith({ include: [], exclude: [], matchMode: "any" });
  });

  it("moves a tag from include to exclude instead of holding both states", () => {
    const onChange = vi.fn();
    render(<Harness initial={{ include: ["dast"], exclude: [], matchMode: "any" }} onChange={onChange} />);
    openPopover();
    fireEvent.click(screen.getByRole("button", { name: "Exclude dast" }));
    expect(onChange).toHaveBeenLastCalledWith({ include: [], exclude: ["dast"], matchMode: "any" });
  });

  it("filters the option list by search text", () => {
    render(<Harness />);
    const list = openPopover();
    fireEvent.change(screen.getByPlaceholderText("Search tags…"), { target: { value: "inc" } });

    expect(within(list).getByRole("option", { name: /inconclusive/ })).toBeInTheDocument();
    expect(within(list).queryByRole("option", { name: /^dast/ })).not.toBeInTheDocument();

    fireEvent.change(screen.getByPlaceholderText("Search tags…"), { target: { value: "zzz" } });
    expect(screen.getByText("No tags match “zzz”")).toBeInTheDocument();
  });

  it("groups system tags separately", () => {
    render(<Harness />);
    const list = openPopover();
    expect(within(list).getByText("System")).toBeInTheDocument();
    expect(within(list).getByRole("option", { name: /aist:duplicate:candidate/ })).toBeInTheDocument();
  });

  it("switches between any-of and all-of for included tags", () => {
    const onChange = vi.fn();
    render(<Harness initial={{ include: ["dast", "cve"], exclude: [], matchMode: "any" }} onChange={onChange} />);
    openPopover();
    fireEvent.click(screen.getByRole("button", { name: "All of" }));
    expect(onChange).toHaveBeenLastCalledWith({ include: ["dast", "cve"], exclude: [], matchMode: "all" });
    expect(screen.getByRole("button", { name: "All of" })).toHaveAttribute("aria-pressed", "true");
  });

  it("removes a single tag from its chip and everything from Clear", () => {
    const onChange = vi.fn();
    render(<Harness initial={{ include: ["dast"], exclude: ["inconclusive"], matchMode: "all" }} onChange={onChange} />);

    fireEvent.click(screen.getByRole("button", { name: "Remove inconclusive" }));
    expect(onChange).toHaveBeenLastCalledWith({ include: ["dast"], exclude: [], matchMode: "all" });

    fireEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(onChange).toHaveBeenLastCalledWith({ include: [], exclude: [], matchMode: "all" });
    expect(screen.getByRole("button", { name: /tags/i })).toHaveTextContent("Any tag");
  });

  it("excludes the highlighted tag on Shift+Enter", () => {
    const onChange = vi.fn();
    render(<Harness onChange={onChange} />);
    openPopover();
    const input = screen.getByPlaceholderText("Search tags…");
    fireEvent.change(input, { target: { value: "inconclusive" } });
    fireEvent.keyDown(input, { key: "Enter", shiftKey: true });
    expect(onChange).toHaveBeenLastCalledWith({ include: [], exclude: ["inconclusive"], matchMode: "any" });
  });

  it("renders the empty label when the project has no tags", () => {
    render(<TagFilter options={[]} value={EMPTY_TAG_FILTER} onChange={() => {}} emptyLabel="No tags available." />);
    openPopover();
    expect(screen.getByText("No tags available.")).toBeInTheDocument();
  });
});
