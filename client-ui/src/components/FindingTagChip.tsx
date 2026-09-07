import * as DropdownMenu from "@radix-ui/react-dropdown-menu";
import { ACCENT_SELECTED_CLASS } from "../lib/uiClasses";
import type { TagFilterState } from "../lib/tagFilter";

type FindingTagChipProps = {
  tag: string;
  state: TagFilterState;
  onInclude: () => void;
  onExclude: () => void;
  onOnly: () => void;
};

const MENU_ITEM_CLASS =
  "flex cursor-pointer select-none items-center gap-2 rounded-lg px-3 py-2 text-sm text-slate-200 outline-none data-[highlighted]:bg-night-700 data-[highlighted]:text-white";

/**
 * A finding's tag as shown in its detail view. Clicking opens a small menu that
 * feeds the page-level tag filter: include, exclude, or keep only this tag.
 */
export default function FindingTagChip({ tag, state, onInclude, onExclude, onOnly }: FindingTagChipProps) {
  return (
    <DropdownMenu.Root modal={false}>
      <DropdownMenu.Trigger asChild>
        <button
          type="button"
          aria-label={`Filter by tag ${tag}`}
          className={[
            "rounded-full border px-3 py-1 text-xs transition outline-none focus-visible:ring-2 focus-visible:ring-brand-600/60 data-[state=open]:ring-2 data-[state=open]:ring-brand-600/60",
            state === "include"
              ? ACCENT_SELECTED_CLASS
              : state === "exclude"
                ? "border-danger-500/50 bg-danger-500/10 text-danger-500 line-through"
                : "border-night-500 bg-night-900 text-slate-200 hover:border-brand-600/40",
          ].join(" ")}
        >
          {tag}
        </button>
      </DropdownMenu.Trigger>
      <DropdownMenu.Portal>
        <DropdownMenu.Content
          align="start"
          sideOffset={6}
          className="z-[1700] min-w-[200px] rounded-xl border border-night-500 bg-night-900 p-1 shadow-panel"
        >
          <DropdownMenu.Label className="px-3 py-1.5 font-mono text-[11px] text-slate-400">{tag}</DropdownMenu.Label>
          <DropdownMenu.Item className={MENU_ITEM_CLASS} onSelect={onInclude}>
            <span aria-hidden="true" className="h-2 w-2 rounded-full bg-brand-500" />
            {state === "include" ? "Stop including" : "Include tag"}
          </DropdownMenu.Item>
          <DropdownMenu.Item className={MENU_ITEM_CLASS} onSelect={onExclude}>
            <span aria-hidden="true" className="h-2 w-2 rounded-full bg-danger-500" />
            {state === "exclude" ? "Stop excluding" : "Exclude tag"}
          </DropdownMenu.Item>
          <DropdownMenu.Item className={MENU_ITEM_CLASS} onSelect={onOnly}>
            <span aria-hidden="true" className="h-2 w-2 rounded-full bg-slate-400" />
            Only this tag
          </DropdownMenu.Item>
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu.Root>
  );
}
