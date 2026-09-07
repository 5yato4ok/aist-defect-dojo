// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import FindingTagChip from "./FindingTagChip";

beforeAll(() => {
  // Radix menus rely on pointer-capture APIs that jsdom does not implement.
  Element.prototype.hasPointerCapture = () => false;
  Element.prototype.releasePointerCapture = () => {};
  Element.prototype.scrollIntoView = vi.fn();
});

afterEach(() => cleanup());

function openMenu() {
  const trigger = screen.getByRole("button", { name: "Filter by tag inconclusive" });
  // Radix DropdownMenu opens on pointerdown, keyboard opens on Enter.
  fireEvent.pointerDown(trigger, { button: 0, ctrlKey: false, pointerType: "mouse" });
  if (!screen.queryByRole("menu")) fireEvent.keyDown(trigger, { key: "Enter" });
  return screen.getByRole("menu");
}

describe("FindingTagChip", () => {
  it("offers include, exclude and only-this-tag for a neutral tag", () => {
    const onInclude = vi.fn();
    const onExclude = vi.fn();
    const onOnly = vi.fn();
    render(<FindingTagChip tag="inconclusive" state="none" onInclude={onInclude} onExclude={onExclude} onOnly={onOnly} />);

    openMenu();
    fireEvent.click(screen.getByRole("menuitem", { name: /Exclude tag/ }));
    expect(onExclude).toHaveBeenCalledTimes(1);
    expect(onInclude).not.toHaveBeenCalled();

    openMenu();
    fireEvent.click(screen.getByRole("menuitem", { name: /Only this tag/ }));
    expect(onOnly).toHaveBeenCalledTimes(1);
  });

  it("labels the actions by the tag's current state", () => {
    render(<FindingTagChip tag="inconclusive" state="exclude" onInclude={() => {}} onExclude={() => {}} onOnly={() => {}} />);
    openMenu();
    expect(screen.getByRole("menuitem", { name: /Stop excluding/ })).toBeInTheDocument();
    expect(screen.getByRole("menuitem", { name: /Include tag/ })).toBeInTheDocument();
  });
});
