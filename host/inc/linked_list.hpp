#pragma once

#include <cstddef>
#include <iterator>
#include <type_traits>


struct LinkedNodeHeader;

template <class T>
struct LinkedElement;

template <typename T, bool IsConst>
struct IteratorOverLink;

template <typename T>
struct LinkedList;


struct LinkedNodeHeader {
protected:
    LinkedNodeHeader *prev, *next;

    template <typename T>
    friend struct LinkedList;
    template <typename T, bool IsConst>
    friend struct IteratorOverLink;

    LinkedNodeHeader() = default;
    explicit LinkedNodeHeader(std::nullptr_t) : prev{this}, next{this} {}
    LinkedNodeHeader(const LinkedNodeHeader&) = delete;
    LinkedNodeHeader& operator=(const LinkedNodeHeader&) = delete;

    void insert_prev(LinkedNodeHeader& to_be_prev)
    {
        to_be_prev.prev = prev;
        to_be_prev.prev->next = &to_be_prev;
        prev = &to_be_prev;
        to_be_prev.next = this;
    }

    void remove()
    {
        prev->next = next;
        next->prev = prev;
        prev = next = this;
    }
    void clear()
    {
        prev = next = this;
    }
};

template <class T>
struct LinkedElement : T, LinkedNodeHeader {
    LinkedElement() = default;
    template <typename... Args>
    explicit LinkedElement(Args&&... args) : T{std::forward<Args>(args)...}, LinkedNodeHeader{nullptr}
    {
    }
    LinkedElement& operator=(const T& other)
    {
        static_cast<T&>(*this) = other;
        return *this;
    }
};

template <typename T, bool IsConst>
struct IteratorOverLink {
    using value_type = std::conditional_t<IsConst, const T, T>;
    using reference = value_type&;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using iterator_category = std::bidirectional_iterator_tag;

    friend struct LinkedList<T>;
    friend struct IteratorOverLink<T, !IsConst>;

private:
    using Node = std::conditional_t<IsConst, const LinkedElement<T>, LinkedElement<T>>;
    using NodeHeader = std::conditional_t<IsConst, const LinkedNodeHeader, LinkedNodeHeader>;
    NodeHeader* elem{nullptr};

    explicit IteratorOverLink(NodeHeader& elem) : elem{&elem} {}

public:
    IteratorOverLink() = default;
    IteratorOverLink(const IteratorOverLink&) = default;
    IteratorOverLink& operator=(const IteratorOverLink&) = default;
    reference operator*() const { return static_cast<Node&>(*elem); }
    pointer operator->() const { return static_cast<Node*>(elem); }
    IteratorOverLink& operator++()
    {
        elem = elem->next;
        return *this;
    }
    IteratorOverLink& operator--()
    {
        elem = elem->prev;
        return *this;
    }

    friend bool operator==(const IteratorOverLink& lhs, const IteratorOverLink& rhs) { return lhs.elem == rhs.elem; }

    IteratorOverLink operator++(int)
    {
        IteratorOverLink tmp{*this};
        ++*this;
        return tmp;
    }
    IteratorOverLink operator--(int)
    {
        IteratorOverLink tmp{*this};
        --*this;
        return tmp;
    }

    friend bool operator!=(const IteratorOverLink& lhs, const IteratorOverLink& rhs) { return !(lhs == rhs); }

    template <bool was_const = IsConst, class = std::enable_if_t<IsConst && !was_const>>
    IteratorOverLink(const IteratorOverLink<T, was_const>& other) : elem{other.elem}
    {
    }
};

template <typename T>
struct LinkedList : LinkedNodeHeader {
    LinkedList() : LinkedNodeHeader{nullptr} {}

    using iterator = IteratorOverLink<T, false>;
    using const_iterator = IteratorOverLink<T, true>;

    iterator begin() { return iterator{*next}; }
    iterator end() { return iterator{*this}; }
    const_iterator begin() const { return const_iterator{*next}; }
    const_iterator end() const { return const_iterator{*this}; }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }

    iterator insert(iterator pos, LinkedElement<T>& elem)
    {
        pos.elem->insert_prev(elem);
        return iterator{elem};
    }
    void push_back(LinkedElement<T>& elem)
    {
        insert(end(), elem);
    }
    void push_front(LinkedElement<T>& elem)
    {
        insert(begin(), elem);
    }

    iterator erase(iterator pos)
    {
        iterator result{*pos.elem->next};
        pos.elem->remove();
        return result;
    }

    void clear()
    {
        LinkedNodeHeader::remove();
    }
};
